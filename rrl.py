import os
from redis import Redis
import datetime
import typing
from dataclasses import dataclass


@dataclass
class Tier:
    name: str
    per_minute: int
    per_hour: int
    per_day: int


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
        self, tiers: typing.List[Tier], *, prefix: str = "", use_redis_time: bool = True
    ):
        self.redis = _get_redis_connection()
        self.tiers = {tier.name: tier for tier in tiers}
        self.prefix = prefix
        self.use_redis_time = use_redis_time

    def check_limit(self, zone: str, key: str, tier_name: str) -> bool:
        if self.use_redis_time:
            timestamp = self.redis.time()[0]
            now = datetime.datetime.fromtimestamp(timestamp)
        else:
            now = datetime.datetime.utcnow()
        tier = self.tiers[tier_name]

        pipe = self.redis.pipeline()
        if tier.per_minute:
            minute_key = f"{self.prefix}:{zone}:{key}:m{now.minute}"
            pipe.incr(minute_key)
            pipe.expire(minute_key, 60)
        if tier.per_hour:
            hour_key = f"{self.prefix}:{zone}:{key}:h{now.hour}"
            pipe.incr(hour_key)
            pipe.expire(hour_key, 3600)
        if tier.per_day:
            day = now.strftime("%Y%m%d")
            day_key = f"{self.prefix}:{zone}:{key}:d{day}"
            pipe.incr(day_key)
            # do not expire day keys for now, useful for metrics
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
