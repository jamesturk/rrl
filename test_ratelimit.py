import datetime
import pytest
from rrl import Tier, RateLimiter, RateLimitExceeded, _get_redis_connection, DailyUsage
from freezegun import freeze_time

redis = _get_redis_connection()
simple_minute_tier = Tier("10/minute", 10, 0, 0)
simple_hour_tier = Tier("10/hour", 0, 10, 0)
simple_daily_tier = Tier("10/day", 0, 0, 10)
long_minute_short_hour_tier = Tier("long_min_short_hour", 100, 10, 0)
everything_set_short_day_tier = Tier("everything_set", 100, 100, 10)
unlimited_tier = Tier("unlimited", 0, 0, 0)


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
        except RateLimitExceeded:
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


def test_get_daily_usage():
    redis.flushall()
    rl = RateLimiter(
        tiers=[unlimited_tier], use_redis_time=False, track_daily_usage=True
    )

    # make Nth day have N calls
    for n in range(1, 10):
        with freeze_time(f"2020-01-0{n}"):
            for _ in range(n):
                rl.check_limit("zone", "test-key", unlimited_tier.name)

    with freeze_time("2020-01-15"):
        usage = rl.get_usage_since("zone", "test-key", datetime.date(2020, 1, 1))
    assert usage[0] == DailyUsage(datetime.date(2020, 1, 1), 1)
    assert usage[3] == DailyUsage(datetime.date(2020, 1, 4), 4)
    assert usage[8] == DailyUsage(datetime.date(2020, 1, 9), 9)
    assert usage[9] == DailyUsage(datetime.date(2020, 1, 10), 0)
    assert usage[14] == DailyUsage(datetime.date(2020, 1, 15), 0)
    assert len(usage) == 15


def test_get_daily_usage_untracked():
    redis.flushall()
    rl = RateLimiter(
        tiers=[unlimited_tier], use_redis_time=False, track_daily_usage=False
    )

    # make Nth day have N calls
    for n in range(1, 10):
        with freeze_time(f"2020-01-0{n}"):
            for _ in range(n):
                rl.check_limit("zone", "test-key", unlimited_tier.name)

    # values would be incorrect (likely zero), warn the caller
    with pytest.raises(RuntimeError):
        rl.get_usage_since("zone", "test-key", datetime.date(2020, 1, 1))
