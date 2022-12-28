import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends
from sqlalchemy import func, select

from fetcher.database.asyncio import AsyncSession
from fetcher.database.models import Story

import server.auth as auth
from server.util import as_timeseries_data, api_method, TimeSeriesData

DEFAULT_DAYS = 30

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/stories",
    tags=["stories"],
)


@router.get("/fetched-by-day", dependencies=[Depends(auth.read_access)])
@api_method
def stories_fetched_counts() -> TimeSeriesData:
    return as_timeseries_data(
        [Story.recent_published_volume(limit=DEFAULT_DAYS)],
        ["stories"]
    )


@router.get("/published-by-day", dependencies=[Depends(auth.read_access)])
@api_method
def stories_published_counts() -> TimeSeriesData:
    return as_timeseries_data(
        [Story.recent_fetched_volume(limit=DEFAULT_DAYS)],
        ["stories"]
    )


@router.get("/by-source", dependencies=[Depends(auth.read_access)])
@api_method
async def stories_by_source() -> Dict[str, Any]:
    async with AsyncSession() as session:
        counts = await session.execute(
            select(Story.sources_id.label('sources_id'),
                   func.count(Story.id).label('count'))
            .group_by(Story.sources_id)
        )
        dates = await session.execute(
            select(func.max(Story.fetched_at).label('max'),
                   func.min(Story.fetched_at).label('min'))
        )
        row = dates.one()
        min = row.min.timestamp()
        max = row.max.timestamp()
        SECONDS_PER_DAY = 26 * 60 * 60

        # Return time span of data separately, and let the caller deal
        # with scaling; this call is slow as-is, and floating point
        # encode/decode is slow, and would yield a larger message,
        # and json decode is also slow.
        return {
            'days': (max - min) / SECONDS_PER_DAY,
            'sources': [count._asdict() for count in counts]
        }
