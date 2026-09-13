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

var pieColors = { shinzo: "#7dd3fc", other: "#475569", free: "#1e293b" };

function formatBytes(n) {
  var units = ["B", "KB", "MB", "GB", "TB"];
  var i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i += 1;
  }
  return n.toFixed(i === 0 ? 0 : 1) + " " + units[i];
}

function renderPie(pieEl, legendEl, segments) {
  var total = segments.reduce(function (sum, s) { return sum + s.bytes; }, 0);
  var cursor = 0;
  var stops = [];
  legendEl.innerHTML = "";

  segments.forEach(function (seg) {
    var pct = total > 0 ? (seg.bytes / total) * 100 : 0;
    var end = cursor + pct;
    stops.push(seg.color + " " + cursor.toFixed(2) + "% " + end.toFixed(2) + "%");
    cursor = end;

    var li = document.createElement("li");
    var swatch = document.createElement("span");
    swatch.className = "swatch";
    swatch.style.background = seg.color;
    li.appendChild(swatch);
    li.appendChild(document.createTextNode(
      seg.label + " — " + formatBytes(seg.bytes) + " (" + pct.toFixed(1) + "%)"
    ));
    legendEl.appendChild(li);
  });

  pieEl.style.background = "conic-gradient(" + stops.join(", ") + ")";
}

fetch("/api/system")
  .then(function (r) {
    if (!r.ok) throw new Error("status " + r.status);
    return r.json();
  })
  .then(function (stats) {
    var disk = stats.disk;
    renderPie(document.getElementById("disk-pie"), document.getElementById("disk-legend"), [
      { label: "Shinzo", bytes: disk.shinzo_bytes, color: pieColors.shinzo },
      { label: "Other used", bytes: Math.max(0, disk.used_bytes - disk.shinzo_bytes), color: pieColors.other },
      { label: "Free", bytes: disk.free_bytes, color: pieColors.free }
    ]);

    var mem = stats.memory;
    renderPie(document.getElementById("memory-pie"), document.getElementById("memory-legend"), [
      { label: "Shinzo", bytes: mem.shinzo_bytes, color: pieColors.shinzo },
      { label: "Other used", bytes: Math.max(0, mem.used_bytes - mem.shinzo_bytes), color: pieColors.other },
      { label: "Free", bytes: mem.free_bytes, color: pieColors.free }
    ]);
  })
  .catch(function (err) {
    ["disk-block", "memory-block"].forEach(function (id) {
      document.getElementById(id).classList.add("err-block");
    });
    console.error("failed to load system stats", err);
  });
