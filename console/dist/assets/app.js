fetch("/api/node")
  .then(function (r) {
    if (!r.ok) throw new Error("status " + r.status);
    return r.json();
  })
  .then(function (info) {
    document.getElementById("node-id").textContent = info.node_id;
    document.getElementById("peer-id").textContent = info.peer_id;
    document.getElementById("did").textContent = info.did;
  })
  .catch(function (err) {
    ["node-id", "peer-id", "did"].forEach(function (id) {
      var el = document.getElementById(id);
      el.textContent = "failed to load";
      el.classList.add("err");
    });
    console.error("failed to load node info", err);
  });
