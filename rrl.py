import os
import datetime
import typing
from dataclasses import dataclass
from redis import Redis


@dataclass
class Tier:
    name: str
    per_minute: int
    per_hour: int
    per_day: int


@dataclass
class DailyUsage:
    date: datetime.date
    calls: int


class RateLimitExceeded(Exception):
    pass


def _get_redis_connection() -> Redis:
    host = os.environ.get("RRL_REDIS_HOST", "localhost")
    port = int(os.environ.get("RRL_REDIS_PORT", 6379))
    db = int(os.environ.get("RRL_REDIS_DB", 0))
    return Redis(host=host, port=port, db=db)


class RateLimiter:
    """
    <zone>:<key>:<hour><minute>         expires in 2 minutes
    <zone>:<key>:<hour>                 expires in 2 hours
    <zone>:<key>:<day>                  never expires
    """

    def __init__(
        self,
        tiers: typing.List[Tier],
        *,
        prefix: str = "",
        use_redis_time: bool = True,
        track_daily_usage: bool = True,
    ):
        self.redis = _get_redis_connection()
        self.tiers = {tier.name: tier for tier in tiers}
        self.prefix = prefix
        self.use_redis_time = use_redis_time
        self.track_daily_usage = track_daily_usage

    def check_limit(self, zone: str, key: str, tier_name: str) -> bool:
        try:
            tier = self.tiers[tier_name]
        except KeyError:
            raise ValueError(f"unknown tier: {tier_name}")
        if self.use_redis_time:
            timestamp = self.redis.time()[0]
            now = datetime.datetime.fromtimestamp(timestamp)
        else:
            now = datetime.datetime.utcnow()

        pipe = self.redis.pipeline()
        if tier.per_minute:
            minute_key = f"{self.prefix}:{zone}:{key}:m{now.minute}"
            pipe.incr(minute_key)
            pipe.expire(minute_key, 60)
        if tier.per_hour:
            hour_key = f"{self.prefix}:{zone}:{key}:h{now.hour}"
            pipe.incr(hour_key)
            pipe.expire(hour_key, 3600)
        if tier.per_day or self.track_daily_usage:
            day = now.strftime("%Y%m%d")
            day_key = f"{self.prefix}:{zone}:{key}:d{day}"
            pipe.incr(day_key)
            # keep data around for usage tracking
            if not self.track_daily_usage:
                pipe.expire(day_key, 86400)
        result = pipe.execute()

        # the result is pairs of results of incr and expire calls, so if all 3 limits are set
        # it looks like [per_minute_calls, True, per_hour_calls, True, per_day_calls]
        # we increment value_pos as we consume values so we know which location we're looking at
        value_pos = 0
        if tier.per_minute:
            if result[value_pos] > tier.per_minute:
                raise RateLimitExceeded(
                    f"exceeded limit of {tier.per_minute}/min: {result[value_pos]}"
                )
            value_pos += 2
        if tier.per_hour:
            if result[value_pos] > tier.per_hour:
                raise RateLimitExceeded(
                    f"exceeded limit of {tier.per_hour}/hour: {result[value_pos]}"
                )
            value_pos += 2
        if tier.per_day:
            if result[value_pos] > tier.per_day:
                raise RateLimitExceeded(
                    f"exceeded limit of {tier.per_day}/day: {result[value_pos]}"
                )

        return True

    def get_usage_since(
        self,
        zone: str,
        key: str,
        start: datetime.date,
        end: typing.Optional[datetime.date] = None,
    ) -> typing.List[DailyUsage]:
        if not self.track_daily_usage:
            raise RuntimeError("track_daily_usage is not enabled")
        if not end:
            end = datetime.date.today()
        days = []
        day = start
        while day <= end:
            days.append(day)
            day += datetime.timedelta(days=1)
        day_keys = [
            f"{self.prefix}:{zone}:{key}:d{day.strftime('%Y%m%d')}" for day in days
        ]
        return [
            DailyUsage(d, int(calls.decode()) if calls else 0)
            for d, calls in zip(days, self.redis.mget(day_keys))
        ]
