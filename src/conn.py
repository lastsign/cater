import redis

pool = redis.ConnectionPool(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True,
    password="your_secure_password",
)
r = redis.Redis(connection_pool=pool)

r.set("foo", "bar")
print(r.get("foo"))
