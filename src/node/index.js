const app = require("express")();
const http = require("http").Server(app);
const io = require("socket.io")(http);
const cassandra = require("cassandra-driver");
const port = 8080;
const refreshRate = 60000;

app.get("/", function (req, res) {
  res.sendFile("index.html", { root: "." });
});

http.listen(port, function () {
  console.log("Running on port " + port);
});

const options = {
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
};

const cassandraClient = new cassandra.Client(options);
cassandraClient.connect(function (err, result) {
  if (err) {
    console.log("Unable to connect to database: ", err.name);
    return;
  }
  console.log("Connected to database");
});

// Create necessary keyspace and tables if they dont exist
cassandraClient
  .execute(
    `CREATE KEYSPACE IF NOT EXISTS access_log WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`
  )
  .then(() =>
    cassandraClient.execute(
      `CREATE TABLE IF NOT EXISTS access_log.searches (keyword text, timestamp timestamp, count int, PRIMARY KEY (keyword, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);`
    )
  )
  .then(() =>
    cassandraClient.execute(
      `CREATE TABLE IF NOT EXISTS access_log.orders (id int, timestamp timestamp, count int, PRIMARY KEY (id, timestamp));`
    )
  .then(() =>
    cassandraClient.execute(
      `CREATE TABLE IF NOT EXISTS access_log.countries (country_code text, timestamp timestamp, count int, PRIMARY KEY (country_code, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);`
    )
  )
  .then(() =>
    cassandraClient.execute(
      `CREATE TABLE IF NOT EXISTS access_log.requests (response_code int, timestamp timestamp, count int, PRIMARY KEY (response_code, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);`
    )
  )
  .then(() =>
    cassandraClient.execute(
      `CREATE TABLE IF NOT EXISTS access_log.os (os text, timestamp timestamp, count int, PRIMARY KEY (os, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);`
    )
  )
  .then(() =>
    cassandraClient.execute(
      `CREATE TABLE IF NOT EXISTS access_log.browsers (browser text, timestamp timestamp, count int, PRIMARY KEY (browser, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);`
    )
  )
);

io.sockets.on("connection", function (socket) {
  console.log("user connected");
  socket.on("disconnect", function () {
    console.log("user disconnected");
  });
});


// Continiously update chart.
setInterval(function () {
  // if (io.engine.clientsCount > 0) {
  cassandraClient.execute(
    "SELECT max(timestamp) AS timestamp, keyword, count FROM access_log.searches GROUP BY keyword;",
    (err, result) => {
      if (err) {
        console.log(err);
        return;
      }
      io.emit("search_data", result.rows);
    }
  );
  // }
}, refreshRate);

// Continiously update chart.
setInterval(function () {
  // if (io.engine.clientsCount > 0) {
  cassandraClient.execute(
    "SELECT * FROM access_log.orders WHERE id = 0 ORDER BY timestamp DESC LIMIT 1;",
    (err, result) => {
      if (err) {
        console.log(err);
        return;
      }
      io.emit("order_data", result.rows[0]);
    }
  );
  // }
}, refreshRate);

// Continiously update chart.
setInterval(function () {
  // if (io.engine.clientsCount > 0) {
  cassandraClient.execute(
    "SELECT max(timestamp) AS timestamp, country_code, count FROM access_log.countries GROUP BY country_code;",
    (err, result) => {
      if (err) {
        console.log(err);
        return;
      }
      io.emit("country_data", result.rows);
    }
  );
  // }
}, refreshRate);

// Continiously update chart.
setInterval(function () {
  // if (io.engine.clientsCount > 0) {
  cassandraClient.execute(
    "SELECT max(timestamp) AS timestamp, response_code, count FROM access_log.requests GROUP BY response_code;",
    (err, result) => {
      if (err) {
        console.log(err);
        return;
      }
      io.emit("requests_count_data", result.rows);
    }
  );
  // }
}, refreshRate);


// Continiously update chart.
setInterval(function () {
  if (io.engine.clientsCount > 0) {
    cassandraClient.execute(
      "SELECT max(timestamp) AS timestamp, os, count FROM access_log.os GROUP BY os;",
      (err, result) => {
        if (err) {
          console.log(err);
          return;
        }
        io.emit("os_data", result.rows);
      }
    );
  }
}, refreshRate);

// Continiously update chart.
setInterval(function () {
  if (io.engine.clientsCount > 0) {
    cassandraClient.execute(
      "SELECT max(timestamp) AS timestamp, browser, count FROM access_log.browsers GROUP BY browser;",
      (err, result) => {
        if (err) {
          console.log(err);
          return;
        }
        io.emit("browser_data", result.rows);
      }
    );
  }
}, refreshRate);