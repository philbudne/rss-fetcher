"""
script invoked by run-fetch-rss-feeds.sh

When run with --loop, stays running as daemon,
sending queue stats, and refreshing the queue.
"""

import datetime as dt
import logging
import sys
import time
from typing import Any, List

# PyPI
from sqlalchemy import or_
from sqlalchemy.orm.query import Query
import sqlalchemy.sql.functions as f

# app
from fetcher.config import conf
from fetcher.database import engine, Session, SessionType
from fetcher.logargparse import LogArgumentParser
import fetcher.database.functions as ff
from fetcher.database.models import Feed, FetchEvent, utc
import fetcher.queue as queue
from fetcher.stats import Stats
import fetcher.tasks as tasks

SCRIPT = 'queue_feeds'          # NOTE! used for stats!
logger = logging.getLogger(SCRIPT)


class Queuer:
    """
    class to encapsulate feed queuing.
    move some place public if needed elsewhere!
    """

    def __init__(self, stats: Stats, wq: queue.Queue):
        self.stats = stats
        self.wq = wq

    def _active_filter(self, q: Query) -> Query:
        """
        filter a feeds query to return only active feeds
        """
        return q.filter(Feed.active.is_(True),
                        Feed.system_enabled.is_(True))

    def _active_feed_ids(self, session: SessionType) -> Query:
        """
        base query to return active feed ids
        """
        return self._active_filter(session.query(Feed.id))

    def count_active(self, session: SessionType) -> int:
        return self._active_feed_ids(session).count()

    def _ready_filter(self, q: Query) -> Query:
        return q.filter(Feed.queued.is_(False),
                        or_(Feed.next_fetch_attempt.is_(None),
                            Feed.next_fetch_attempt <= utc()))

    def _ready_query(self, session: SessionType) -> Query:
        """
        return base query for feed id's ready to be fetched
        """
        return self._ready_filter(self._active_feed_ids(session))

    def find_and_queue_feeds(self, limit: int) -> int:
        """
        Find some active, undisabled, unqueued feeds
        that have not been checked, or are past due for a check (oldest first).
        """
        if limit > conf.MAX_FEEDS:
            limit = conf.MAX_FEEDS

        now = dt.datetime.utcnow()

        # Maybe order by (id % 100) instead of id
        #  to help break up clumps?
        with Session.begin() as session:
            # NOTE nulls_first is preferred in sqlalchemy 1.4
            #  but not available in sqlalchemy-stubs 0.4

            # maybe secondary order by (Feed.id % 1001)?
            #  would require adding adding a column to query
            rows = self._ready_filter(
                self._active_filter(session.query(Feed.id)))\
                .order_by(Feed.next_fetch_attempt.asc().nullsfirst(),
                          Feed.id.desc())\
                .limit(limit)\
                .all()  # all rows
            feed_ids = [row[0] for row in rows]
            if not feed_ids:
                return 0

            # mark as queued first so that workers can never see
            # a feed_id that hasn't been marked as queued.
            session.query(Feed)\
                   .filter(Feed.id.in_(feed_ids))\
                   .update({'last_fetch_attempt': now, 'queued': True},
                           synchronize_session=False)

            # create a fetch_event row for each feed:
            for feed_id in feed_ids:
                # created_at value matches Feed.last_fetch_attempt
                # (when queued) and queue entry
                session.add(
                    FetchEvent.from_info(feed_id,
                                         FetchEvent.Event.QUEUED,
                                         now))
        return self.queue_feeds(feed_ids, now.isoformat())

    def queue_feeds(self, feed_ids: List[int], ts_iso: str) -> int:
        queued = queue.queue_feeds(self.wq, feed_ids, ts_iso)
        total = len(feed_ids)
        # XXX report total-queued as separate (labled) counter?
        self.stats.incr('queued_feeds', queued)

        logger.info(f"Queued {queued}/{total} feeds")
        return queued


def fetches_per_minute(session: SessionType) -> int:
    """
    Return average expected fetches per minute, based on
    Feed.update_minutes (derived from <sy:updatePeriod> and
    <sy:updateFrequency>).

    NOTE!! This needs to be kept in sync with the policy in
    fetcher.tasks.update_feed()!!!
    """
    return int(
        session.query(
            f.sum(
                1.0 /
                ff.greatest(    # never faster than minimum interval
                    f.coalesce(  # use DEFAULT if update_minutes is NULL
                        Feed.update_minutes,
                        conf.DEFAULT_INTERVAL_MINS),
                    conf.MINIMUM_INTERVAL_MINS
                )  # greatest
            )  # sum
        ).one()[0]
    )

# XXX make a queuer method? should only be used here!


def loop(queuer: Queuer, refill_period_mins: int = 5) -> None:
    """
    Loop monitoring & reporting queue length to stats server

    Try to spread out load (smooth out any lumps),
    keeps queue short (db changes seen quickly)
    Initial randomization of next_fetch_attempt in import process
    will _initially_ avoid lumpiness, but downtime will cause
    pileups that will take at most MINIMUM_INTERVAL_MINS
    to clear (given enough workers).
    """

    logger.info(f"Starting loop: refill every {refill_period_mins} min")
    db_ready = hi_water = -1
    while True:
        t0 = time.time()        # wake time
        # logger.debug(f"top {t0}")

        # always report queue stats (inexpensive with rq):
        qlen = queue.queue_length(queuer.wq)  # queue(r) method??
        active = queue.queue_active(queuer.wq)  # jobs in progress
        workers = queue.queue_workers(queuer.wq)  # active workers

        # NOTE: initial qlen (not including added)
        #       active entries are NOT included in qlen
        queuer.stats.gauge('qlen', qlen)
        queuer.stats.gauge('active', active)
        queuer.stats.gauge('workers', workers)
        logger.debug(f"qlen {qlen} active {active} workers {workers}")

        added = 0

        # always queue on startup, then
        # wait for multiple of refill_period_mins.
        if (hi_water < 0 or
                (int(t0 / 60) % refill_period_mins) == 0):

            # name hi_water is a remnant of an implementation attempt
            # that refilled to hi_water only when queue drained to lo_water.

            # hi_water is the number of fetches per refill_period_mins
            # that need to be performed.  Enforcing this average means
            # that any "bunching" of ready feeds (due to outage) will
            # be spread out evenly.

            # Only putting as much work as needs to be done in
            # refill_period_mins means that database changes
            # (additions, enables, disables) can be seen quickly
            # rather than waiting for the whole queue to drain.

            with Session() as session:
                hi_water = fetches_per_minute(session) * refill_period_mins

            # for dev/debug, on small databases:
            if hi_water < 10:
                hi_water = 10

            queuer.stats.gauge('hi_water', hi_water)

            # if queue is below the limit, fill up to the limit.
            if qlen < hi_water:
                added = queuer.find_and_queue_feeds(hi_water - qlen)

        # gauges "stick" at last value, so always set:
        queuer.stats.gauge('added', added)

        # BEGIN MAYBE MOVE:
        # queries done once a minute for monitoring only!
        # if this is a problem move this section up
        # (under ... % refill_period_mins == 0)
        # statsd Gauges assume the value they are set to,
        # until they are set to a new value.

        # after find_and_queue_feeds, so does not include "added" entries
        with Session() as session:
            # all entries marked active and enabled.
            # there is probably a problem if more than a small
            #  fraction of active entries are ready!
            db_active = queuer.count_active(session)

            # should be approx (updated) qlen + active
            db_queued = session.query(Feed)\
                               .filter(Feed.queued.is_(True))\
                               .count()

            db_ready = queuer._ready_query(session).count()

        queuer.stats.gauge('db.active', db_active)
        queuer.stats.gauge('db.queued', db_queued)
        queuer.stats.gauge('db.ready', db_ready)

        logger.debug(
            f" db active {db_active} queued {db_queued} ready {db_ready}")
        # END MAYBE MOVE

        tnext = (t0 - t0 % 60) + 60  # top of the next minute after wake time
        t1 = time.time()
        s = tnext - t1             # sleep time
        if s > 0:
            # logger.debug(f"t1 {t1} tnext {tnext} sleep {s}")
            time.sleep(s)


if __name__ == '__main__':
    p = LogArgumentParser(SCRIPT, 'Feed Queuing')
    p.add_argument('--clear', action='store_true',
                   help='Clear queue and exit.')
    p.add_argument('--loop', action='store_true',
                   help='Clear queue and run as daemon, sending stats.')
    p.add_argument('feeds', metavar='FEED_ID', nargs='*', type=int,
                   help='Fetch specific feeds')

    # info logging before this call unlikely to be seen:
    args = p.my_parse_args()       # parse logging args, output start message

    stats = Stats.init(SCRIPT)

    wq = queue.workq()

    queuer = Queuer(stats, wq)

    if args.clear:
        logger.info("Clearing Queue")
        queue.clear_queue()
        sys.exit(0)

    if args.loop:
        # log early
        _ = conf.MINIMUM_INTERVAL_MINS
        _ = conf.DEFAULT_INTERVAL_MINS
        _ = conf.MAX_FEEDS

        if args.feeds:
            logger.error('Cannot give both --loop and feed ids')
            sys.exit(1)

        logger.info("Clearing Queue")
        queue.clear_queue()

        loop(queuer)            # should never return
        sys.exit(1)             # should not get here

    # support passing in one or more feed ids on the command line
    if args.feeds:
        feed_ids = [int(feed) for feed in args.feeds]
        with Session.begin() as session:
            # validate ids
            rows = queuer._ready_query(session)\
                         .filter(Feed.id.in_(feed_ids))\
                         .all()
            valid_ids = [row[0] for row in rows]
        # maybe complain about invalid feeds??
        #   find via set(feed_ids) - set(valid_feeds)
        nowstr = dt.datetime.utcnow().isoformat()
        queuer.queue_feeds(valid_ids, nowstr)
    else:
        # classic behavior (was run from cron every 30 min)
        # to restore, uncomment crontab entry in instance.sh
        # and remove --loop from Procfile
        queuer.find_and_queue_feeds(conf.MAX_FEEDS)
