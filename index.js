// db
db = {};

// graph
db.g = {};

db.graph = function (v, e) {
  let graph = Object.create(db.g);

  graph.edges = [];
  graph.vertices = [];
  graph.vertexIndex = {};

  graph.autoId = 1;

  if (Array.isArray(v)) graph.addVertices(v);
  if (Array.isArray(e)) graph.addEdges(e);

  return graph;
};

db.g.addVertices = function (v) {
  v.forEach(this.addVertex.bind(this));
};
db.g.addEdges = function (e) {
  e.forEach(this.addEdge.bind(this));
};

db.g.addVertex = function (v) {
  if (!v._id) {
    v._id = this.autoId++;
  } else if (this.findVertexById(v._id)) {
    return db.error("a vertex with same id already exists");
  }

  this.vertices.push(v);
  this.vertexIndex[v._id] = v;
  v._out = [];
  v._in = [];

  return v._id;
};

db.g.addEdge = function (e) {
  e._in = this.findVertexById(e._in);
  e._out = this.findVertexById(e._out);

  if (!(e._in && e._out))
    return db.error(
      "that edge's " + (e._in ? "out" : "in") + " vertex not found",
    );

  e._out._out.push(e);
  e._in._in.push(e);

  this.edges.push(e);
};

db.g.v = function () {
  let query = db.query(this);
  query.add("vertex", [].slice.call(arguments));
  return query;
};

db.g.findVertices = function (args) {
  if (typeof args[0] === "object") {
    return this.searchVertices(args[0]);
  } else if (args.length == 0) {
    return this.vertices.slice();
  } else {
    return this.findVerticesByIds(args);
  }
};

db.g.findVerticesByIds = function (ids) {
  if (ids.length == 1) {
    let maybe_vertex = this.findVertexById(ids[0]);
    return maybe_vertex ? [maybe_vertex] : [];
  }

  return ids.map(this.findVertexById.bind(this)).filter(Boolean);
};

db.g.findVertexById = function (vertex_id) {
  return this.vertexIndex[vertex_id];
};

db.g.searchVertices = function (filter) {
  return this.vertices.filter(function (filter) {
    return db.objectFilter(vertex, filter);
  });
};

db.g.findInEdges = function (v) {
  return v._in;
};

db.g.findOutEdges = function (v) {
  return v._out;
};

db.g.toString = function () {
  return db.jsonify(this);
};

// query
db.q = {};

db.query = function (g) {
  let query = Object.create(db.q);

  query.graph = g;
  query.state = [];
  query.program = [];
  query.gremlins = [];

  return query;
};

db.q.add = function (pipetype, args) {
  let step = [pipetype, args];
  this.program.push(step);
  return this;
};

// interpreter

db.q.run = function () {
  this.program = db.transform(this.program);

  let max = this.program.length - 1;
  let maybe_gremlin = false;
  let results = [];
  let done = -1;
  let pc = max;

  let step, state, pipetype;

  while (done < max) {
    let ts = this.state;
    step = this.program[pc];
    state = ts[pc] = ts[pc] || {};
    pipetype = db.getPipetype(step[0]);
    maybe_gremlin = pipetype(this.graph, step[1], maybe_gremlin, state);
    if (maybe_gremlin == "pull") {
      maybe_gremlin = false;
      if (pc - 1 > done) {
        pc--;
        continue;
      } else {
        done = pc;
      }
    }
    if (maybe_gremlin == "done") {
      maybe_gremlin = false;
      done = pc;
    }
    pc++;

    if (pc > max) {
      if (maybe_gremlin) results.push(maybe_gremlin);
      maybe_gremlin = false;
      pc--;
    }
  }

  results = results.map(function (gremlin) {
    return gremlin.result != null ? gremlin.result : gremlin.vertex;
  });

  return results;
};

// pipetypes
db.pipetypes = {};

db.addPipetype = function (name, fn) {
  db.pipetypes[name] = fn;

  db.q[name] = function () {
    return this.add(name, [].slice.apply(arguments));
  };
};

db.getPipetype = function (name) {
  let pipetype = db.pipetypes[name];

  if (!pipetype) db.error("unrecognized pipetype: " + name);

  return pipetype || db.fauxPipetype;
};

db.fauxPipetype = function (_, _, maybe_gremlin) {
  return maybe_gremlin || "pull";
};

db.simpleTraversal = function (dir) {
  let find_method = dir == "out" ? "findOutEdges" : "findInEdges";
  let edge_list = dir == "out" ? "_in" : "_out";

  return function (graph, args, gremlin, state) {
    if (!gremlin && (!state.edges || !state.edges.length)) return "pull";

    if (!state.edges || !state.edges.length) {
      state.gremlin = gremlin;
      state.edges = graph[find_method](gremlin.vertex).filter(
        db.filterEdges(args[0]),
      );
    }

    if (!state.edges.length) return "pull";

    let vertex = state.edges.pop()[edge_list];
    return db.gotoVertex(state.gremlin, vertex);
  };
};

// error
db.error = function (msg) {
  console.error(msg);
  return fasle;
};

// adding pipetypes
db.addPipetype("vertex", function (graph, args, gremlin, state) {
  if (!state.vertices) state.vertices = graph.findVertices(args);

  if (!state.vertices.length) return "done";

  let vertex = state.vertices.pop();
  return db.makeGremlin(vertex, gremlin.state);
});

db.addPipetype("out", db.simpleTraversal("out"));
db.addPipetype("in", db.simpleTraversal("in"));

db.addPipetype("property", function (graph, args, gremlin, state) {
  if (!gremlin) return "pull";
  gremlin.result = gremlin.vertex[args[0]];
  return gremlin.result == null ? false : gremlin;
});

db.addPipetype("unique", function (graph, args, gremlin, state) {
  if (!gremlin) return "pull";
  if (state[gremlin.vertex._id]) return "pull";
  state[gremlin.vertex._id] = true;
  return gremlin;
});

db.addPipetype("filter", function (graph, args, gremlin, state) {
  if (!gremlin) return "pull";

  if (typeof args[0] == "object")
    return db.objectFilter(gremlin.vertex, args[0]) ? gremlin : "pull";

  if (typeof args[0] != "function") {
    db.error("filter is not a function: " + args[0]);
    return gremlin;
  }

  if (!args[0](gremlin.vertex, gremlin)) return "pull";

  return gremlin;
});

db.addPipetype("take", function (graph, args, gremlin, state) {
  state.taken = state.taken || 0;

  if (state.taken == args[0]) {
    state.taken = 0;
    return "done";
  }

  if (!gremlin) return "pull";
  state.taken++;

  return gremlin;
});

db.addPipetype("as", function (graph, args, gremlin, state) {
  if (!gremlin) return "pull";
  gremlin.state.as = gremlin.state.as || {};
  gremlin.state.as[args[0]] = gremlin.vertex;
  return gremlin;
});

db.addPipetype("merge", function (graph, args, gremlin, state) {
  if (!state.vertices && !gremlin) return "pull";

  if (!state.vertices || !state.vertices.length) {
    let obj = (gremlin.state || {}).as || {};
    state.vertices = args
      .map(function (id) {
        return obj[id];
      })
      .filter(Boolean);
  }

  if (!state.vertices.length) return "pull";

  let vertex = state.vertices.pop();
  return db.makeGremlin(vertex, gremlin.state);
});

db.addPipetype("except", function (graph, args, gremlin, state) {
  if (!gremlin) return "pull";
  if (gremlin.vertex == gremlin.state.as[args[0]]) return "pull";
  return gremlin;
});

db.addPipetype("back", function (graph, args, gremlin, state) {
  if (!gremlin) return "pull";
  return db.gotoVertex(gremlin, gremlin.state.as[args[0]]);
});

// helperks
db.makeGremlin = function (vertex, state) {
  return { vertex, state: state || {} };
};

db.gotoVertex = function (gremlin, vertex) {
  return db.makeGremlin(vertex, gremlin.state);
};

db.filterEdges = function (filter) {
  return function (edge) {
    if (!filter) return true;

    if (typeof filter == "string") return edge._label == filter;

    if (Array.isArray(filter)) return !!~filter.indexOf(edge._label);

    return db.objectFilter(edge, filter);
  };
};

db.objectFilter = function (thing, filter) {
  for (let key in filter) if (thing[key] !== filter[key]) return false;

  return true;
};

// transformers

db.t = [];

db.addTransformer = function (fn, priority) {
  if (typeof fn != "function") return db.error("invalid transformer function");

  let i = 0;
  for (; i < db.t.length; i++) if (priority > db.t[i].priority) break;

  db.t.splice(i, 0, { priority, fn });
};

db.transform = function (program) {
  return db.t.reduce(function (acc, transformer) {
    return transformer.fn(acc);
  }, program);
};

// aliases

db.addAlias = function (newName, oldName, defaults) {
  defaults = defaults || [];

  db.addTransformer(function (program) {
    return program.map(function (step) {
      if (step[0] !== newName) return step;
      return [oldName, db.extend(step[1], defaults)];
    });
  }, 100);

  db.addPipetype(newName, function () {});
};

db.extend = function (list, defaults) {
  return Object.keys(defaults).reduce(function (acc, key) {
    if (typeof list[key] != "undefined") return acc;
    acc[key] = defaults[key];
    return acc;
  }, list);
};

db.addAlias("grandparents", [
  ["out", "parent"],
  ["out", "parent"],
]);
db.addAlias("siblings", [
  ["as", "me"],
  ["out", "parent"],
  ["in", "parent"],
  ["except", "me"],
]);
db.addAlias("cousins", [
  "parents",
  ["as", "folks"],
  "parents",
  "children",
  ["except", "folks"],
  "children",
  "unique",
]);

// serialization

db.jsonify = function (graph) {
  return (
    '{"V":' +
    JSON.stringify(graph.vertices, db.cleanVertex) +
    ', "E":' +
    JSON.stringify(graph.edges, db.cleanEdge) +
    "}"
  );
};

db.cleanVertex = function (key, value) {
  return key == "_in" || key == "_out" ? value._id : value;
};

db.cleanEdge = function (key, value) {
  return key == "_in" || key == "_out" ? value._id : value;
};

db.fromString = function (str) {
  let obj = JSON.parse(str);
  return db.graph(obj.v, obj.e);
};

// persistence

db.persist = function (graph, name) {
  name = name || "graph";
  localStorage.setItem("db::" + name, graph);
};

db.depersist = function (name) {
  name = "db::" + (name || "graph");
  let graph = localStorage.getItem(name);
  return db.fromString(graph);
};

console.log(db);
