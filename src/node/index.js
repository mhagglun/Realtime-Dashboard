const app = require("express")();
const http = require("http").Server(app);
const io = require("socket.io")(http);
const cassandra = require("cassandra-driver");
const port = 8080;

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
      `CREATE TABLE IF NOT EXISTS access_log.searches (keyword text PRIMARY KEY, count float);`
    )
  )
  .then(() =>
    cassandraClient.execute(
      "CREATE TABLE IF NOT EXISTS access_log.orders (word text PRIMARY KEY, count float);"
    )
  .then(() =>
    cassandraClient.execute(
      `CREATE TABLE IF NOT EXISTS access_log.countries (country_code text PRIMARY KEY, count float);`
    )
  )
);

io.sockets.on("connection", function (socket) {
  console.log("User connected");
  socket.on("disconnect", function () {
    console.log("user disconnected");
  });
});


// TODO: Replace by reading data from output kafka topic instead,
// Only load data directly from db when instantiating view

// Continiously update chart.
setInterval(function () {
  if (io.engine.clientsCount > 0) {
    cassandraClient.execute(
      "SELECT * FROM access_log.searches;",
      (err, result) => {
        if (err) return;
        io.emit("search_data", result.rows);
      }
    );
  }
}, 1000); //update every sec.

// Continiously update chart.
setInterval(function () {
  if (io.engine.clientsCount > 0) {
    cassandraClient.execute(
      "SELECT * FROM access_log.orders;",
      (err, result) => {
        if (err) return;
        io.emit("order_data", result.rows);
      }
    );
  }
}, 1000); //update every sec.

// Continiously update chart.
setInterval(function () {
  if (io.engine.clientsCount > 0) {
    cassandraClient.execute(
      "SELECT * FROM access_log.countries;",
      (err, result) => {
        if (err) return;
        io.emit("country_data", result.rows);
      }
    );
  }
}, 1000); //update every sec.