# rrl - Redis Rate Limiting

Very simple reusable redis-backed rate limiting code.  Designed for OpenStates.org.

![Test](https://github.com/jamesturk/rrl/workflows/Test/badge.svg)

## Configuration

`rrl`, as the name implies, requires a [Redis](https://redis.io) server.

`rrl` is entirely configured by environment variables:

**RRL_REDIS_HOST** - hostname of Redis instance (default: *localhost*)
**RRL_REDIS_PORT** - port of Redis instance (default: *6379*)
**RRL_REDIS_DB** - database ID to use for RRL (default: *0*)

## Usage

Usage can be throttled on a per-minute, per-hour, and/or per-day basis.

`rrl` has the concept of a `Tier` which associates a name with a set of limitations, for instance:

```
# create two tiers
bronze = Tier(name="bronze", per_minute=1, per_hour=0, per_day=500)
silver = Tier(name="silver", per_minute=5, per_hour=0, per_day=4000)
```

These tiers do not use the per_hour feature, but will limit users to 1 or 5 requests per minute respectively.  There's also a daily limit at 500 or 4000 requests per day.

Then you'll need an instance of `rrl.RateLimiter`, which will be instantiated with these tiers:

```
limiter = RateLimiter(tiers=[bronze, silver])
```

Then to apply limiting, you'll call the `check_limit` function, which takes three parameters:

* `key` - A unique-per user key, often the user's API key or username. (Note: `rrl` does not know if a key is valid or not, that validation should be in your application and usually occur before the call to `check_limit`.)
* `tier_name` - The name of one of the tiers as specified when instantiating the `RateLimiter` class.  (Note: `rrl` does not have a concept of which users are in which tier, that logic should be handled by your key validation code.)

Example call:

```
limiter.check_limit(key="1234", tier_name="bronze")
```

This call will return without any error if the call is deemed allowed.

If any of the rate limits are exceeded it will instead raise a `RateLimitExceeded` exception describing which limit was exceeded.  
If multiple limits were exceeded it will return the shortest limit violated.

## Advanced Usage

### Obtaining Usage Information

Your `RateLimiter` instance also has a method named `get_usage_since`, which takes four parameters:

* `key` - Which key you're requesting usage information for.
* `start` - Date that you'd like usage since, as a `datetime.date` object.
* `end` - Optional end date if you'd only like usage within a certain window, otherwise the current day is used.

This will return a list of `DailyUsage` dataclasses with the following attributes:

* `date` - `datetime.date`
* `calls` - Number of calls made on that date.

This method can be useful for showing users an overview of their data.

### Advanced Configuration

When instantiating a `RateLimiter` there are several keyword-only parameters you may set:

**prefix**

Passing a prefix like: 
```
limiter = RateLimiter(tiers, prefix="v1")
```
will scope all calls to limiter to a given prefix, this can be useful if you want multiple limiters but want to ensure that they do not interfere with one another.

**use_redis_time**

`True` by default, but if you set to `False` the application's system time will be used instead. 

The tradeoff here is one fewer call to Redis per call to `check_limit`, but if your machines experience any clock drift unexpected results may occur.

**track_daily_usage**

`True` by default, but if set to `False`, `rrl` will not store the necessary information to make `get_usage_since` work.  This results in a slight overhead reduction, but the usage information will not be stored in Redis and will be impossible to retrieve.
