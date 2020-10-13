import pytest
from rrl import Tier, RateLimiter, RateLimitExceeded, _get_redis_connection
from freezegun import freeze_time

redis = _get_redis_connection()
simple_minute_tier = Tier("10/minute", 10, 0, 0)
simple_hour_tier = Tier("10/hour", 0, 10, 0)
simple_daily_tier = Tier("10/day", 0, 0, 10)
long_minute_short_hour_tier = Tier("long_min_short_hour", 100, 10, 0)
everything_set_short_day_tier = Tier("everything_set", 100, 100, 10)


@pytest.mark.parametrize(
    "tier,reset_time",
    [
        (simple_minute_tier, 60),
        (simple_hour_tier, 3600),
        (simple_daily_tier, 60 * 60 * 25),
        (long_minute_short_hour_tier, 3600),
        (everything_set_short_day_tier, 60 * 60 * 25),
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


def test_using_redis_time():
    redis.flushall()
    rl = RateLimiter(tiers=[simple_daily_tier], use_redis_time=True)

    # don't loop infinitely if test is failing
    count = 0
    while count < 20:
        try:
            rl.check_limit("test-zone", "test-key", simple_daily_tier.name)
            count += 1
        except RateLimitExceeded as e:
            break
    assert count == 10


def test_multiple_zones():
    redis.flushall()
    rl = RateLimiter(tiers=[simple_daily_tier], use_redis_time=True)

    # don't loop infinitely if test is failing
    count = 0
    while count < 20:
        try:
            rl.check_limit("zone1", "test-key", simple_daily_tier.name)
            rl.check_limit("zone2", "test-key", simple_daily_tier.name)
            count += 1
        except RateLimitExceeded:
            break
    assert count == 10


def test_multiple_keys():
    redis.flushall()
    rl = RateLimiter(tiers=[simple_daily_tier], use_redis_time=True)

    # don't loop infinitely if test is failing
    count = 0
    while count < 20:
        try:
            rl.check_limit("zone", "test-key1", simple_daily_tier.name)
            rl.check_limit("zone", "test-key2", simple_daily_tier.name)
            count += 1
        except RateLimitExceeded:
            break
    assert count == 10
