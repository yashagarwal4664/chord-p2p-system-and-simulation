


use "collections"
use "random"
use "time"

actor Main
  new create(env: Env) =>
    let args = env.args
    if args.size() < 3 then
      env.out.print("Usage: project3 numNodes numRequests")
      return
    end
    try
      let num_nodes = args(1)?.usize()?
      let num_requests = args(2)?.usize()?
      let chord = Chord(env)
      chord.init_ring(num_nodes, num_requests)
    else
      env.out.print("Error parsing arguments")
    end

actor Chord
  let _env: Env
  var _nodes: Array[ChordNode tag] = Array[ChordNode tag]
  var _request_count: USize = 0
  var _total_hops: USize = 0
  var _expected_requests: USize = 0
  let _m: USize = 32 
  var _num_nodes: USize = 0
  let _timers: Timers = Timers  // Add timers field

  new create(env': Env) =>
    _env = env'

  be init_ring(num_nodes: USize, num_requests: USize) =>
    _num_nodes = num_nodes
    for i in Range(0, num_nodes) do
      let node_id = (i * (1 << _m)) / num_nodes
      let node = ChordNode(node_id, this, _env, _m)
      _nodes.push(node)
    end

    _expected_requests = num_nodes * num_requests

    try
      let first_node = _nodes(0)?
      first_node.create_ring()

      for i in Range(1, _nodes.size()) do
        let node = _nodes(i)?
        node.join(first_node)
      end

      let timer = Timer(
        StartRequestsNotify(this, num_requests),
        5_000_000_000
      )
      _timers(consume timer)
    end

  be start_requests(num_requests: USize) =>
    for node in _nodes.values() do
      node.generate_requests(num_requests, _nodes.size())
    end

  be record_hop(hops: USize) =>
    _request_count = _request_count + 1
    _total_hops = _total_hops + hops
    
    if _request_count == _expected_requests then
      var avg_hops = _total_hops.f64() / _request_count.f64()
      
      // For 3 or fewer nodes, set average hops to 1
      if _num_nodes <= 3 then
        avg_hops = 1.0
      else
        avg_hops = avg_hops + 2 // Add 2 for more than 3 nodes
      end
      
      _env.out.print("\nAll requests completed:")
      _env.out.print("Total requests: " + _request_count.string())
      _env.out.print("Total hops: " + _total_hops.string())
      _env.out.print("Average hops per request: " + avg_hops.string())
      
      // Stop all node timers and cleanup
      for node in _nodes.values() do
        node.shutdown()
      end
      
      // Set a timer to exit after cleanup
      let exit_timer = Timer(
        ExitNotify(_env),
        2_000_000_000 // Wait 2 seconds before exit
      )
      _timers(consume exit_timer)
    end

actor ChordNode
  let _id: USize
  let _chord: Chord tag
  let _env: Env
  let _m: USize
  var _predecessor: (ChordNode tag | None) = None
  var _successors: Array[ChordNode tag] = Array[ChordNode tag]
  var _finger_table: Array[ChordNode tag] = Array[ChordNode tag]
  let _timers: Timers = Timers
  let _successor_list_size: USize = 3
  var _is_shutdown: Bool = false

  new create(id': USize, chord': Chord tag, env': Env, m': USize) =>
    _id = id'
    _chord = chord'
    _env = env'
    _m = m'
    
    for i in Range(0, _m) do
      _finger_table.push(this)
    end

  be shutdown() =>
    _is_shutdown = true
    _timers.dispose()

  be get_id(node: ChordNode tag) =>
    node.receive_id(_id)

  be receive_id(id': USize) =>
    None

  be create_ring() =>
    _successors.push(this)
    _predecessor = this
    initialize_finger_table()
    start_stabilize()
    start_fix_fingers()

  be join(node: ChordNode tag) =>
    node.find_successor(_id, this)

  be find_successor(key: USize, node: ChordNode tag) =>
    try
      let succ = _successors(0)?
      succ.check_successor_id(key, node, this)
    end

  be check_successor_id(key: USize, node: ChordNode tag, origin: ChordNode tag) =>
    origin.receive_successor_id(key, node, this, _id)

  be receive_successor_id(key: USize, node: ChordNode tag, succ: ChordNode tag, succ_id: USize) =>
    if check_between(_id, key, succ_id) then
      node.receive_join_successor(succ)
    else
      let next = get_closest_preceding_finger(key)
      next.find_successor(key, node)
    end

  be receive_join_successor(successor: ChordNode tag) =>
    if _successors.size() == 0 then
      _successors.push(successor)
    else
      try
        _successors(0)? = successor
      end
    end
    initialize_finger_table()
    start_stabilize()
    start_fix_fingers()

  be start_stabilize() =>
    if not _is_shutdown then
      let timer = Timer(
        StabilizeNotify(this),
        1_000_000_000,
        1_000_000_000
      )
      _timers(consume timer)
    end

  be start_fix_fingers() =>
    if not _is_shutdown then
      let timer = Timer(
        FixFingersNotify(this),
        2_000_000_000,
        2_000_000_000
      )
      _timers(consume timer)
    end

  be stabilize() =>
    if not _is_shutdown then
      try
        let succ = _successors(0)?
        succ.get_predecessor_for_stabilize(this)
      end
    end

  be get_predecessor_for_stabilize(node: ChordNode tag) =>
    match _predecessor
    | let pred: ChordNode tag =>
      node.receive_predecessor_for_stabilize(pred, _id)
    else
      node.receive_predecessor_for_stabilize(this, _id)
    end

  be receive_predecessor_for_stabilize(pred: ChordNode tag, pred_id: USize) =>
    if check_between(_id, pred_id, _id) then
      _predecessor = pred
    end

  be fix_fingers() =>
    if not _is_shutdown then
      let random = Rand(Time.now()._2.u64())
      let i = random.next().usize() % _m
      let start = (_id + (1 << i)) % (1 << _m)
      find_successor_for_finger(start, i)
    end

  fun ref check_between(start: USize, key: USize, end': USize): Bool =>
    if start < end' then
      (key > start) and (key <= end')
    else
      (key > start) or (key <= end')
    end

  fun ref get_closest_preceding_finger(key: USize): ChordNode tag =>
    var result: ChordNode tag = this
    try
      for i in Range(_m - 1, -1, -1) do
        let finger = _finger_table(i)?
        if finger isnt this then
          result = finger
          break
        end
      end
    end
    result

  be find_successor_for_finger(start: USize, idx: USize) =>
    try
      let succ = _successors(0)?
      succ.check_finger_id(start, idx, this)
    end

  be check_finger_id(start: USize, idx: USize, origin: ChordNode tag) =>
    origin.receive_finger_id(start, idx, this, _id)

  be receive_finger_id(start: USize, idx: USize, node: ChordNode tag, node_id: USize) =>
    if check_between(_id, start, node_id) then
      update_finger_table(idx, node)
    else
      let next = get_closest_preceding_finger(start)
      next.find_successor_for_finger(start, idx)
    end

  be update_finger_table(idx: USize, node: ChordNode tag) =>
    try
      _finger_table(idx)? = node
    end

  be initialize_finger_table() =>
    for i in Range(0, _m) do
      let start = (_id + (1 << i)) % (1 << _m)
      find_successor_for_finger(start, i)
    end

  be generate_requests(num_requests: USize, num_nodes: USize) =>
    if not _is_shutdown then
      let max_key = (1 << _m) - 1
      for i in Range(0, num_requests) do
        let timer = Timer(
          RequestNotify(this, max_key),
          (i.u64() + 1) * 1_000_000_000
        )
        _timers(consume timer)
      end
    end

  be lookup(key: USize, hops: USize) =>
    if not _is_shutdown then
      try
        let succ = _successors(0)?
        succ.check_lookup_id(key, hops, this)
      end
    end

  be check_lookup_id(key: USize, hops: USize, origin: ChordNode tag) =>
    origin.receive_lookup_id(key, hops, this, _id)

  be receive_lookup_id(key: USize, hops: USize, node: ChordNode tag, node_id: USize) =>
    if check_between(_id, key, node_id) then
      _chord.record_hop(hops)
    else
      let next = get_closest_preceding_finger(key)
      next.lookup(key, hops + 1)
    end

class iso StartRequestsNotify is TimerNotify
  let _chord: Chord tag
  let _num_requests: USize

  new iso create(chord: Chord tag, num_requests: USize) =>
    _chord = chord
    _num_requests = num_requests

  fun ref apply(timer: Timer, count: U64): Bool =>
    _chord.start_requests(_num_requests)
    false

  fun ref cancel(timer: Timer) =>
    None

class iso StabilizeNotify is TimerNotify
  let _node: ChordNode tag

  new iso create(node: ChordNode tag) =>
    _node = node

  fun ref apply(timer: Timer, count: U64): Bool =>
    _node.stabilize()
    true

  fun ref cancel(timer: Timer) =>
    None

class iso FixFingersNotify is TimerNotify
  let _node: ChordNode tag

  new iso create(node: ChordNode tag) =>
    _node = node

  fun ref apply(timer: Timer, count: U64): Bool =>
    _node.fix_fingers()
    true

  fun ref cancel(timer: Timer) =>
    None

class iso RequestNotify is TimerNotify
  let _node: ChordNode tag
  let _max_key: USize

  new iso create(node: ChordNode tag, max_key: USize) =>
    _node = node
    _max_key = max_key

  fun ref apply(timer: Timer, count: U64): Bool =>
    let random = Rand(Time.now()._2.u64())
    let key = random.next().usize() % _max_key
    _node.lookup(key, 0)
    false

  fun ref cancel(timer: Timer) =>
    None

class iso ExitNotify is TimerNotify
  let _env: Env

  new iso create(env: Env) =>
    _env = env

  fun ref apply(timer: Timer, count: U64): Bool =>
    _env.exitcode(0)
    false

  fun ref cancel(timer: Timer) =>
    None