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


class RateLimitException(Exception):
    pass


class RateLimiter:
    """
    <zone>:<key>:<hour><minute>         expires in 2 minutes
    <zone>:<key>:<hour>                 expires in 2 hours
    <zone>:<key>:<day>                  never expires
    """

    def __init__(self, prefix: str, tiers: typing.List[Tier]):
        self.redis = Redis()
        self.tiers = {tier.name: tier for tier in tiers}
        self.prefix = prefix

    def check_limit(self, zone: str, key: str, tier_name: str):
        timestamp = self.redis.time()[0]
        now = datetime.datetime.fromtimestamp(timestamp)
        tier = self.tiers[tier_name]

        pipe = self.redis.pipeline()

        if tier.per_minute:
            minute_key = f"{self.prefix}:{zone}:{key}:m{now.minute}"
            calls = pipe.incr(minute_key)
            pipe.expire(minute_key, 60)
            if calls > tier.per_minute:
                raise RateLimitException(f"exceeded limit of {tier.per_minute}/min")
        if tier.per_hour:
            hour_key = f"{self.prefix}:{zone}:{key}:h{now.hour}"
            calls = pipe.incr(hour_key)
            pipe.expire(hour_key, 3600)
            if calls > tier.per_hour:
                raise RateLimitException(f"exceeded limit of {tier.per_hour}/hour")
        if tier.per_day:
            day = now.strftime("%Y%m%d")
            day_key = f"{self.prefix}:{zone}:{key}:d{day}"
            calls = pipe.incr(day_key)
            # do not expire day keys for now, useful for metrics
            if calls > tier.per_day:
                raise RateLimitException(f"exceeded limit of {tier.per_day}/day")
