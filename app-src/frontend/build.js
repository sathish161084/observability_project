const fs = require('fs');
const html = `<!doctype html><html><head><meta charset="utf-8"><title>Three Tier Demo</title>
<style>body{font-family:Arial;margin:2rem;background:#f6f8fb;color:#223}button{margin-right:10px;margin-bottom:10px;padding:10px 16px}.card{background:white;padding:1rem;border-radius:8px;margin-bottom:1rem;box-shadow:0 1px 6px rgba(0,0,0,.08)}pre{background:#111827;color:#e5e7eb;padding:1rem;border-radius:8px}</style></head>
<body><h1>Three Tier Observability Demo</h1><div class="card">
<button onclick="listProducts()">List Products</button>
<button onclick="checkout(false)">Checkout Success</button>
<button onclick="checkout(true)">Checkout Worker Failure</button>
<button onclick="slow()">Slow Request</button>
<button onclick="backendError()">Backend Error</button>
</div><div class="card"><pre id="output">No calls yet</pre></div>
<script>
function base(){return window.location.origin.includes('localhost')?'http://localhost:8000':''}
function show(d){document.getElementById('output').textContent=JSON.stringify(d,null,2)}
async function listProducts(){const r=await fetch(base()+'/api/products');show(await r.json())}
async function checkout(simulateFailure){const r=await fetch(base()+'/api/checkout',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({customer_id:'cust-100',amount:249.99,simulate_failure:simulateFailure})});show(await r.json())}
async function slow(){const r=await fetch(base()+'/api/slow');show(await r.json())}
async function backendError(){const r=await fetch(base()+'/api/checkout',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({customer_id:'cust-500',amount:20,force_error:true})});show(await r.json())}
</script></body></html>`;
fs.mkdirSync('dist',{recursive:true});fs.writeFileSync('dist/index.html', html);
