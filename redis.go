package main

import (
	"log"

	"github.com/gomodule/redigo/redis"
)

type RedisApi struct {
	pool *redis.Pool
}

func (r *RedisApi) connect() error {
	log.Println("Initializing redis pool.")
	r.pool = &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return nil, err
			}

			return c, err
		},
		MaxIdle: 100,
	}

	return r.pool.Get().Err()
}

func (r *RedisApi) disconnect() error {
	log.Println("Closing redis pool.")
	return r.pool.Close()
}

func (r *RedisApi) save(key string, value string) {
	log.Println("Saving to redis.", key, value)
	conn := r.pool.Get()
	defer conn.Close()
	_, err := conn.Do("HSET", key, value, "")
	if err != nil {
		log.Println(err)
	}
}

func (r *RedisApi) read(key string) []string {
	result := []string{}
	conn := r.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("HKEYS", key))
	if err != nil {
		log.Println(err)
	}
	redis.ScanSlice(values, &result)

	log.Println("Values for", key, "found in redis:", result)
	return result
}

func (r *RedisApi) remove(key string, value string) {
	log.Println("Deleting from redis:", key, value)
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("HDEL", key, value)
	if err != nil {
		log.Println(err)
	}
}

func (r *RedisApi) addToIndex(index string, topic string) {
	log.Println("Adding to redis index", index, topic)
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SADD", index, topic)
	if err != nil {
		log.Println(err)
	}
}

func (r *RedisApi) getIndex(key string) []string {
	result := []string{}
	conn := r.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("SMEMBERS", key))
	if err != nil {
		log.Println(err)
	}
	redis.ScanSlice(values, &result)
	log.Println("Index", key, "found in redis", result)
	return result
}

func (r *RedisApi) isSubscribed(session string, topic string) bool {
	conn := r.pool.Get()
	defer conn.Close()

	res, err := redis.Int(conn.Do("HEXISTS", session, topic))
	if err != nil {
		log.Println(err)
	}

	if res == 1 {
		return true
	} else {
		return false
	}
}
