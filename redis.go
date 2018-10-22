package main

import (
	"log"

	"github.com/gomodule/redigo/redis"
)

type RedisApi struct {
	pool *redis.Pool
}

func (r *RedisApi) Connect() error {
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

func (r *RedisApi) Disconnect() error {
	log.Println("Closing redis pool.")
	return r.pool.Close()
}

func (r *RedisApi) Save(key string, value string) error {
	log.Println("Saving to redis.", key, value)
	conn := r.pool.Get()
	defer conn.Close()
	_, err := conn.Do("HSET", key, value, "")
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (r *RedisApi) Read(key string) ([]string, error) {
	result := []string{}
	conn := r.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("HKEYS", key))
	if err != nil {
		log.Println(err)
		return nil, err
	}
	redis.ScanSlice(values, &result)

	log.Println("Values for", key, "found in redis:", result)
	return result, nil
}

func (r *RedisApi) Remove(key string, value string) error {
	log.Println("Deleting from redis:", key, value)
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("HDEL", key, value)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (r *RedisApi) AddToIndex(index string, topic string) error {
	log.Println("Adding to redis index", index, topic)
	conn := r.pool.Get()
	defer conn.Close()

	_, err := conn.Do("SADD", index, topic)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (r *RedisApi) GetIndex(key string) ([]string, error) {
	result := []string{}
	conn := r.pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("SMEMBERS", key))
	if err != nil {
		log.Println(err)
		return nil, err
	}
	redis.ScanSlice(values, &result)
	log.Println("Index", key, "found in redis", result)
	return result, nil
}

func (r *RedisApi) IsSubscribed(session string, topic string) (bool, error) {
	conn := r.pool.Get()
	defer conn.Close()

	res, err := redis.Int(conn.Do("HEXISTS", session, topic))
	if err != nil {
		log.Println(err)
		return false, err
	}

	if res == 1 {
		return true, nil
	} else {
		return false, nil
	}
}
