class Recommendify::SparseMatrix

  def initialize(opts={})
    @opts = opts
  end

  def redis_key
    [@opts.fetch(:redis_prefix), @opts.fetch(:key)].join(":")
  end

  def [](x,y)
    k_get(key(x,y))
  end

  def []=(x,y,v)
    v == 0 ? k_del(key(x,y)) : k_set(key(x,y), v)
  end

  def incr(x,y)
    k_incr(key(x,y))
  end

  def set_set_id_items(set_id, items)
    Recommendify.redis.hset(set_id_key, set_id, items.join(","))
  end

  def add_to_set_id_items(set_id, new_item)
    current_items = get_set_id_items(set_id)
    new_items = current_items << new_item
    Recommendify.redis.hset(set_id_key, set_id, new_items.join(","))
  end

  def get_set_id_items(set_id)
    current = Recommendify.redis.hget(set_id_key, set_id) || ""
    current.split(",").map(&:to_s)
  end

  def queue_for_processing(item_id)
    Recommendify.redis.rpush(processing_queue_key, item_id)
  end

  def each_process_queue_item
    while item_id = Recommendify.redis.lpop(processing_queue_key)
      yield item_id
    end
  end

private

  def set_id_key
    "#{@opts.fetch(:redis_prefix)}:set_id_items"
  end

  def processing_queue_key
    "#{@opts.fetch(:redis_prefix)}:processing_queue"
  end

  def key(x,y)
    [x,y].sort.join(":")
  end

  def k_set(key, val)
    Recommendify.redis.hset(redis_key, key, val)
  end

  def k_del(key)
    Recommendify.redis.hdel(redis_key, key)
  end

  def k_get(key)
    Recommendify.redis.hget(redis_key, key).to_f
  end

  def k_incr(key)
    Recommendify.redis.hincrby(redis_key, key, 1)
  end

  # OPTIMIZE: use scripting/lua in redis 2.6
  def k_delall(*keys)
    Recommendify.redis.hkeys(redis_key).each do |iikey|
      next unless (iikey.split(":") & keys).size > 0
      Recommendify.redis.hdel(redis_key, iikey)
    end
  end

end
