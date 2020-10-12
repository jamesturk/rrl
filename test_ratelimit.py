import pytest
from ratelimit import Tier, RateLimiter, RateLimitExceeded
from freezegun import freeze_time
from redis import Redis

redis = Redis()
simple_minute_tier = Tier("minute", 10, 0, 0)
simple_hour_tier = Tier("hour", 0, 10, 0)
simple_daily_tier = Tier("day", 0, 0, 10)


@pytest.mark.parametrize(
    "tier,reset_time",
    [
        (simple_minute_tier, 60),
        (simple_hour_tier, 3600),
        (simple_daily_tier, 60 * 60 * 25),
    ],
)
def test_check_limit_per_minute(tier, reset_time):
    redis.flushall()
    rl = RateLimiter(tiers=[tier], use_redis_time=False)

    count = 0
    with freeze_time() as frozen:
        # don't loop infinitely if test is failing
        while count < 20:
            try:
                rl.check_limit("test-zone", "test-key", tier.name)
                count += 1
            except RateLimitExceeded as e:
                print(e)
                break
        # assert that we broke after 10
        assert count == 10
        # resets after a given time
        frozen.tick(reset_time)
        assert rl.check_limit("test-zone", "test-key", tier.name)
