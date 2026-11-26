/**
 * 星光影库 · 登录态网盘站（独立站 v1）
 * ------------------------------------------------------------
 * 与之前那批「登录站」最大的不同：
 *   1. 完全不调用 /spider/pansou 的任何路由，解析/取链/代理全在本文件内实现
 *   2. 代理路由按「玩偶」的写法自带：/spider/xkykvip/3/proxy/raw/<自包含签名>
 *      签名里带着 网盘类型+分享ID+文件ID+token，进程重启后老地址照样能播
 *   3. 多线程加速：16 路并发 × 512KB 分块，首块 8×64KB 先抢出来；
 *      被网盘限流(429/403)会自动降到 6 路，空闲 60 秒再慢慢加回 16
 *
 * 依赖的配置（猫源【配置】里登录网盘后自动写入）：
 *   config.uc.cookie     ← 主要资源，星光影库九成是 UC 分享
 *   config.quark.cookie  ← 有夸克分享时才会用到
 *
 * 播放链路：play 只回代理地址 → 播放器首个 Range 请求到达时才去换直链
 *   （换链结果缓存 7 分钟，同一个文件的 seek 直接命中内存缓存，2ms 内响应）
 */
module.exports = (function () {
  "use strict";

  var http = require("http");
  var https = require("https");

  var KEY = "xkykvip";
  var TITLE = "✨ 星光影库·登录";

  /* 激进参数：并发 16 路 × 512KB，首块 8×64KB 先抢出来 */
  var RL_POOL = 16;
  var RL_CHUNK = 512 * 1024;
  var RL_HEAD = 512 * 1024;
  var RL_HW = 8;
  var RL_HS = 64 * 1024;
  var RL_CACHE = 256 * 1024 * 1024;
  var RL_PRELOAD = 32 * 1024 * 1024;
  var RL_PW = 12;

  var agent = new https.Agent({ keepAlive: true, maxSockets: 160, maxFreeSockets: 48 });

  function parseUrl(u) { try { return new URL(u); } catch (e) { return null; } }

  function rawReq(u, opt) {
    opt = opt || {};
    return new Promise(function (resolve, reject) {
      var p = parseUrl(u);
      if (!p) return reject(new Error("bad url"));
      var lib = p.protocol === "http:" ? http : https;
      var headers = Object.assign({}, opt.headers || {});
      var body = opt.body;
      if (body != null) headers["Content-Length"] = Buffer.byteLength(body);
      var done = false, killer = null, wd = null;
      var r = lib.request({
        protocol: p.protocol, hostname: p.hostname,
        port: p.port || (p.protocol === "http:" ? 80 : 443),
        path: p.pathname + p.search, method: opt.method || "GET",
        headers: headers,
        agent: opt.noAgent ? false : (p.protocol === "https:" ? (opt.agent || agent) : undefined)
      }, function (res) {
        /* 注意：stream 模式绝不能在这里挂 data 监听——那会把流切到 flowing 模式，
           数据在调用方拿到 res 之前就被消费掉（表现为只收到最后几十字节）。 */
        if (opt.watchdog && !opt.stream) {
          wd = setTimeout(function () { try { res.destroy(); } catch (e) { } }, opt.watchdog);
          res.on("data", function () { if (wd) { clearTimeout(wd); wd = null; } });
        }
        if (opt.stream) {
          done = true; if (killer) clearTimeout(killer);
          if (res.statusCode === 429 && opt.onBusy) opt.onBusy(429);
          return resolve(res);
        }
        var chunks = [];
        res.on("data", function (c) { chunks.push(c); });
        res.on("end", function () {
          if (done) return;
          done = true; if (killer) clearTimeout(killer); if (wd) clearTimeout(wd);
          var buf = Buffer.concat(chunks);
          try {
            var ce = String(res.headers["content-encoding"] || "");
            if (/gzip/i.test(ce)) buf = require("zlib").gunzipSync(buf);
            else if (/deflate/i.test(ce)) buf = require("zlib").inflateSync(buf);
            else if (/br/i.test(ce)) buf = require("zlib").brotliDecompressSync(buf);
          } catch (e) { }
          resolve({ status: res.statusCode, headers: res.headers, buf: buf, text: buf.toString("utf8") });
        });
        res.on("error", function () { if (!done) { done = true; if (killer) clearTimeout(killer); if (wd) clearTimeout(wd); reject(new Error("res error")); } });
      });
      r.on("timeout", function () { try { r.destroy(); } catch (e) { } });
      r.on("error", function (e) { if (!done) { done = true; if (killer) clearTimeout(killer); if (wd) clearTimeout(wd); reject(e); } });
      var tmo = opt.timeout === 0 ? 0 : (opt.timeout || 20000);
      if (tmo > 0) {
        killer = setTimeout(function () {
          try { r.destroy(); } catch (e) { }
          if (!done) { done = true; reject(new Error("timeout")); }
        }, tmo);
      }
      if (body != null) r.write(body);
      r.end();
    });
  }

  function jsonOf(res) {
    if (!res) return null;
    try { return JSON.parse(res.text); } catch (e) { return null; }
  }

  function postJson(u, obj, h, opt) {
    return rawReq(u, Object.assign({
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json; charset=utf-8" }, h || {}),
      body: JSON.stringify(obj || {})
    }, opt || {}));
  }
  var XK_BASE = "https://xkykmovie.com";
  var XK_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
  var XK_PAGE = 24;
  var XK_CATS = [
    ["movie", "🎬 电影", "电影"],
    ["tv", "📺 剧集", "电视剧"],
    ["anime", "🌸 动漫", "动漫"],
    ["show", "🎤 综艺", "综艺"],
    ["doc", "🌍 纪录片", "纪录片"]
  ];
  var XK_GENRES = ["动作", "喜剧", "爱情", "科幻", "悬疑", "惊悚", "犯罪", "剧情", "动画", "奇幻", "冒险",
    "战争", "恐怖", "家庭", "历史", "音乐", "纪录片", "真人秀", "古装", "武侠"];
  var XK_AREAS = ["大陆", "香港", "台湾", "美国", "日本", "韩国", "英国", "法国", "德国", "意大利",
    "西班牙", "印度", "泰国", "俄罗斯", "加拿大", "澳大利亚"];

  var XK_AREA_MAP = {
    CN: "大陆", "中国大陆": "大陆", China: "大陆", "中国": "大陆", "华语": "大陆", "内地": "大陆", "大陆": "大陆",
    HK: "香港", "香港": "香港", TW: "台湾", "台湾": "台湾",
    US: "美国", "美国": "美国", JP: "日本", "日本": "日本", KR: "韩国", "韩国": "韩国",
    GB: "英国", "英国": "英国", FR: "法国", "法国": "法国", DE: "德国", "德国": "德国",
    IT: "意大利", "意大利": "意大利", ES: "西班牙", "西班牙": "西班牙",
    IN: "印度", "印度": "印度", TH: "泰国", "泰国": "泰国",
    RU: "俄罗斯", SU: "俄罗斯", "俄罗斯": "俄罗斯",
    CA: "加拿大", "加拿大": "加拿大", AU: "澳大利亚", "澳大利亚": "澳大利亚"
  };

  var XK_ATTR_RE = {};
  function xkAttr(tag, name) {
    var re = XK_ATTR_RE[name] || (XK_ATTR_RE[name] = new RegExp("\\s" + name + '="([^"]*)"'));
    var m = re.exec(tag);
    return m ? xkUnesc(m[1]) : "";
  }

  function xkUnesc(s) {
    return String(s == null ? "" : s)
      .replace(/&#(\d+);/g, function (_, d) { return String.fromCharCode(Number(d)); })
      .replace(/&#x([0-9a-fA-F]+);/g, function (_, h) { return String.fromCharCode(parseInt(h, 16)); })
      .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&apos;/g, "'")
      .replace(/&nbsp;/g, " ").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
      .replace(/&amp;/g, "&");
  }

  function xkText(s) {
    s = String(s || "").replace(/<[^>]*>/g, " ");
    return xkUnesc(s).replace(/\s+/g, " ").trim();
  }

  function xkArea(v) {
    v = String(v || "").trim();
    if (!v) return "";
    return XK_AREA_MAP[v] || "其他";
  }

  /* 探索页里每张卡片都带 data-* 属性，一次正则扫完就是全站目录 */
  function xkParse(html) {
    var out = [], re = /<article[^>]*class="[^"]*movie-card[^"]*"[\s\S]*?<\/article>/g, m;
    while ((m = re.exec(html))) {
      var c = m[0];
      var id = xkAttr(c, "data-resource-id") || xkAttr(c, "data-id");
      var title = xkAttr(c, "data-title") || xkAttr(c, "data-resource-title");
      if (!id || !title) continue;
      var tags = [];
      try { tags = JSON.parse(xkAttr(c, "data-tags") || "[]"); } catch (e) { tags = []; }
      if (!Array.isArray(tags)) tags = [];
      var img = /<img[^>]+src="([^"]+)"/.exec(c);
      out.push({
        id: id,
        name: title,
        cat: xkAttr(c, "data-category"),
        area: xkArea(xkAttr(c, "data-country")),
        tags: tags.filter(function (x) { return typeof x === "string" && x; }),
        year: xkAttr(c, "data-year"),
        score: parseFloat(xkAttr(c, "data-score")) || 0,
        created: Date.parse(xkAttr(c, "data-created-at")) || 0,
        pic: img ? xkUnesc(img[1]) : "",
        playable: xkAttr(c, "data-playable") === "true",
        savable: xkAttr(c, "data-savable") === "true"
      });
    }
    return out;
  }

  function xkSort(list, by) {
    var arr = list.slice();
    if (by === "score") {
      arr.sort(function (a, b) { return b.score - a.score || xkYear(b) - xkYear(a); });
    } else if (by === "created") {
      /* 真正按入库时间，站里刚补的片子会先冒头 */
      arr.sort(function (a, b) { return b.created - a.created || xkYear(b) - xkYear(a); });
    } else {
      /* 默认：有网盘的优先，再看年份 —— 老片批量导入的时间戳都很新，纯按入库会满屏 1950 年代 */
      arr.sort(function (a, b) {
        var sa = a.savable ? 1 : 0, sb = b.savable ? 1 : 0;
        if (sa !== sb) return sb - sa;
        return xkYear(b) - xkYear(a) || b.created - a.created || b.score - a.score;
      });
    }
    return arr;
  }

  function xkYear(x) { return parseInt(x && x.year, 10) || 0; }

  function xkRemark(x) {
    var bits = [];
    if (x.year) bits.push(x.year);
    if (x.score) bits.push("★" + x.score.toFixed(1));
    bits.push(x.savable ? "网盘" : "待补");
    return bits.join(" · ");
  }

  function xkVods(items, limit, req, key) {
    var out = (items || []).slice(0, limit || 60).map(function (x) {
      return {
        vod_id: x.id,
        vod_name: x.name,
        vod_pic: xkPic(x.pic, req, key),
        vod_remarks: xkRemark(x)
      };
    });
    /* 列表一生成就让服务器先把前几张图抓下来，客户端滚到时已经是本地缓存 */
    xkPicWarm(out);
    return out;
  }

  /* ---------------- 封面图：本地代理 + 内存缓存 ----------------
     tmdb 的 image.tmdb.org 国内直连基本打不开，wsrv（Cloudflare）能用但会随机被掐，
     客户端自己拉图没有重试，于是「有的能出来有的出不来」。
     这里改成：图片地址指回站点自己的 /pic 路由，服务器抓一次缓存住（失败换候选链重试），
     客户端拿到的永远是本地地址，第二次直接命中缓存。 */
  /* images.tmdb.org 是 tmdb 图床的反代（BunnyCDN），国内能直连，
     客户端自己拉比绕本站代理快一个数量级 —— 优先直连，探活失败才回退到本地代理 */
  var XK_IMG_BASE = "https://images.tmdb.org";
  var XK_PIC_MODE = { m: "direct", at: 0 };

  var XK_PIC_CACHE = {}, XK_PIC_ORDER = [], XK_PIC_BYTES = 0, XK_PIC_BUSY = {};
  var XK_PIC_SEM = 0, XK_PIC_QUEUE = [], XK_PIC_MAX = 28;

  function xkSlot() {
    if (XK_PIC_SEM < XK_PIC_MAX) { XK_PIC_SEM++; return Promise.resolve(); }
    return new Promise(function (r) { XK_PIC_QUEUE.push(r); });
  }
  function xkFree() {
    var n = XK_PIC_QUEUE.shift();
    if (n) n(); else XK_PIC_SEM--;
  }

  function xkSize(w) {
    w = parseInt(w, 10) || 342;
    if (w <= 120) return "w92";
    if (w <= 200) return "w185";
    if (w <= 400) return "w342";
    return "w500";
  }

  /* 把 image.tmdb.org 的地址换成反代域名，并按需要换成列表/详情用的尺寸 */
  function xkDirect(u, w) {
    var s = String(u || "").replace(/^https?:\/\/[^/]+/i, XK_IMG_BASE);
    var seg = xkSize(w);
    if (/\/t\/p\/w\d+(_and_h\d+_face)?/.test(s)) s = s.replace(/\/t\/p\/w\d+(_and_h\d+_face)?/, "/t/p/" + seg);
    else if (/\/t\/p\//.test(s)) s = s.replace(/\/t\/p\//, "/t/p/" + seg + "/");
    return s;
  }

  /* 图床探活：能直连就用直连，连不上才切回本地代理（结果缓存 20 分钟） */
  function xkProbePic() {
    if (XK_PIC_MODE.at && Date.now() - XK_PIC_MODE.at < 20 * 60 * 1000) return Promise.resolve(XK_PIC_MODE.m);
    var probe = XK_IMG_BASE + "/t/p/w92/78lPtwv72eTNqFW9COBYI0dWDJa.jpg";
    var t0 = Date.now();
    return rawReq(probe, { headers: { "User-Agent": XK_UA }, timeout: 4500 }).then(function (r) {
      var ok = r && r.status === 200 && /^image\//i.test(String((r.headers && r.headers["content-type"]) || ""));
      /* 通但慢（9KB 花了 4 秒以上）也算不可用，改走本站代理 */
      if (ok && Date.now() - t0 > 4000) ok = false;
      XK_PIC_MODE.m = ok ? "direct" : "proxy";
      XK_PIC_MODE.at = Date.now();
      return XK_PIC_MODE.m;
    }, function () {
      XK_PIC_MODE.m = "proxy"; XK_PIC_MODE.at = Date.now();
      return "proxy";
    });
  }

  function xkCdn(u, w) {
    w = w || 92;
    return "https://wsrv.nl/?url=" + encodeURIComponent(String(u).replace(/^https?:\/\//i, "")) + "&w=" + w + "&output=jpg&q=76";
  }

  /* 候选链：wsrv → weserv → tmdb 原图 → media.tmdb（新版图床，走另一套 CDN） */
  function xkCands(u, w) {
    u = String(u || "").trim();
    w = w || 92;
    var bare = u.replace(/^https?:\/\//i, "");
    var q = "&w=" + w + "&output=jpg&q=76";
    var cands = [
      "https://wsrv.nl/?url=" + encodeURIComponent(bare) + q,
      "https://images.weserv.nl/?url=" + encodeURIComponent(bare) + q,
      u
    ];
    if (/image\.tmdb\.org/i.test(u)) {
      cands.push(u.replace(/image\.tmdb\.org/i, "media.themoviedb.org"));
    }
    var seen = {}, out = [];
    cands.forEach(function (x) { if (x && !seen[x]) { seen[x] = 1; out.push(x); } });
    return out;
  }

  function xkFetchPic(u, w) {
    var cands = xkCands(u, w);
    function attempt(i) {
      if (i >= cands.length) return Promise.resolve(null);
      return xkSlot().then(function () {
        return rawReq(cands[i], {
          headers: {
            "User-Agent": XK_UA,
            "Accept": "image/avif,image/webp,image/jpeg,image/*;q=0.8",
            "Referer": XK_BASE + "/"
          },
          timeout: 9000
        }).then(function (r) {
          xkFree();
          var ct = String((r && r.headers && r.headers["content-type"]) || "");
          if (r && r.status >= 200 && r.status < 300 && r.buf && r.buf.length > 800 && /^image\//i.test(ct)) {
            return { buf: r.buf, ct: ct.split(";")[0] };
          }
          return attempt(i + 1);
        }, function () { xkFree(); return attempt(i + 1); });
      });
    }
    return attempt(0);
  }

  function xkPutPic(sig, im) {
    if (!im || !im.buf || !im.buf.length || im.buf.length > 1500000) return;
    var cur = XK_PIC_CACHE[sig];
    if (cur) { XK_PIC_BYTES -= cur.buf.length; }
    else { XK_PIC_ORDER.push(sig); }
    XK_PIC_CACHE[sig] = { buf: im.buf, ct: im.ct, at: Date.now() };
    XK_PIC_BYTES += im.buf.length;
    while ((XK_PIC_BYTES > 26 * 1024 * 1024 || XK_PIC_ORDER.length > 420) && XK_PIC_ORDER.length) {
      var k = XK_PIC_ORDER.shift();
      var v = XK_PIC_CACHE[k];
      if (v) { XK_PIC_BYTES -= v.buf.length; delete XK_PIC_CACHE[k]; }
    }
  }

  function xkPicGet(sig, w) {
    w = Math.max(120, Math.min(600, parseInt(w, 10) || 240));
    var ck = sig + "#" + w;
    var hit = XK_PIC_CACHE[ck];
    if (hit) return Promise.resolve(hit);
    if (XK_PIC_BUSY[ck]) return XK_PIC_BUSY[ck];
    var u = "";
    try { u = Buffer.from(String(sig), "base64url").toString("utf8"); } catch (e) { u = ""; }
    if (!/^https?:\/\//i.test(u)) return Promise.resolve(null);
    var task = xkFetchPic(u, w).then(function (im) {
      delete XK_PIC_BUSY[ck];
      if (im) xkPutPic(ck, im);
      return im;
    }, function () { delete XK_PIC_BUSY[ck]; return null; });
    XK_PIC_BUSY[ck] = task;
    return task;
  }

  /* 从列表里挑前几张先抓，滚屏时就不用等 */
  function xkPicWarm(list) {
    var n = 0;
    (list || []).forEach(function (v) {
      if (n >= 24) return;
      var pic = String((v && v.vod_pic) || "");
      var i = pic.indexOf("/3/pic/");
      if (i < 0) return;
      var tail = pic.slice(i + 7);
      var qm = tail.indexOf("?");
      var sig = qm >= 0 ? tail.slice(0, qm) : tail;
      var w = qm >= 0 ? (/[?&]w=(\d+)/.exec(tail.slice(qm)) || [])[1] : 0;
      if (!sig) return;
      n++;
      xkPicGet(sig, w).catch(function () { });
    });
  }

  /* tmdb 的图国内直连不通、CDN 还会随机失败 → 统一回到站点本地代理（失败时 302 回 CDN） */
  function xkPic(u, req, key, w) {
    u = String(u || "").trim();
    if (!u) return "";
    if (!/^https?:\/\//i.test(u)) return u;
    if (u.indexOf("/spider/") >= 0) return u;
    w = w || 92;
    /* 直连反代图床：客户端自己并发拉，不排本站的队 */
    if (XK_PIC_MODE.m === "direct") return xkDirect(u, w);
    var host = "";
    try { host = (req && req.headers && (req.headers.host || req.headers.Host)) || ""; } catch (e) { host = ""; }
    if (!host) return xkCdn(u, w);
    var root = /^https?:\/\//i.test(host) ? host : "http://" + host;
    return root + "/spider/" + (key || "xkykfree") + "/3/pic/" + Buffer.from(u, "utf8").toString("base64url") + "?w=" + w;
  }

  function xkYears() {
    var out = [], y = new Date().getFullYear() + 1;
    for (var i = 0; i < 12; i++) out.push({ n: String(y - i), v: String(y - i) });
    return out;
  }

  function xkFilters() {
    var years = [{ n: "全部", v: "" }].concat(xkYears());
    var genres = [{ n: "全部", v: "" }].concat(XK_GENRES.map(function (g) { return { n: g, v: g }; }));
    var areas = [{ n: "全部", v: "" }].concat(XK_AREAS.map(function (g) { return { n: g, v: g }; }));
    var f = {};
    XK_CATS.forEach(function (c) {
      f[c[0]] = [
        { key: "class", name: "类型", value: genres },
        { key: "area", name: "地区", value: areas },
        { key: "year", name: "年份", value: years },
        { key: "by", name: "排序", value: [{ n: "最新", v: "" }, { n: "最近入库", v: "created" }, { n: "评分", v: "score" }] }
      ];
    });
    return f;
  }

  function xkHint(id, msg, meta, req, key) {
    meta = meta || {};
    return {
      vod_id: String(id), vod_name: meta.name || String(id), vod_pic: xkPic(meta.pic, req, key, 92),
      vod_content: meta.content || "",
      vod_play_from: "提示", vod_play_url: String(msg || "没有可用线路") + "$none"
    };
  }

  /* ================= 网盘直链（自建，不碰核心 pansou） ================= */

  var QK_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) quark-cloud-drive/2.5.20 Chrome/100.0.4896.160 Electron/18.3.5.4-b478491100 Safari/537.36 Channel/pckk_other_ch";
  var UC_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) uc-cloud-drive/2.5.20 Chrome/100.0.4896.160 Electron/18.3.5.4-b478491100 Safari/537.36 Channel/pckk_other_ch";

  var PANS = {
    quark: {
      k: "quark", name: "夸克", tag: "⚡高速",
      base: "https://drive-pc.quark.cn/1/clouddrive/", q: "pr=ucpro&fr=pc",
      ua: QK_UA, ref: "https://pan.quark.cn", ck: ["quark", "夸克"]
    },
    uc: {
      k: "uc", name: "UC", tag: "⚡高速",
      base: "https://pc-api.uc.cn/1/clouddrive/", q: "pr=UCBrowser&fr=pc",
      ua: UC_UA, ref: "https://drive.uc.cn", ck: ["uc", "UC"]
    }
  };

  var QK_VIDEO = /\.(mp4|mkv|ts|mov|flv|avi|wmv|m4v|webm|rmvb|mpg|mpeg|m2ts|f4v|vob|3gp|asf)$/i;

  var PAN_RES = [
    ["UC", /https?:\/\/drive\.uc\.cn\/s\/[0-9a-zA-Z]+(?:\?[^"'<>\s]*)?/],
    ["夸克", /https?:\/\/pan\.quark\.cn\/s\/[0-9a-zA-Z]+(?:\?[^"'<>\s]*)?/],
    ["123", /https?:\/\/(?:www\.)?(?:123pan|123684|123865|123912|123592)\.(?:com|cn)\/s\/[\w\-]+(?:\?[^"'<>\s]*)?/]
  ];

  function confOf(e, req) {
    var s = (req && req.server) || e || {};
    if (s.config) return s.config;
    if (s.server && s.server.config) return s.server.config;
    return {};
  }
  function cookieOf(conf, k) {
    if (!conf) return "";
    var a = conf[k];
    if (a && typeof a === "object" && a.cookie) return String(a.cookie);
    if (typeof a === "string" && a.length > 20) return a;
    try {
      var ps = conf.pans || conf.panList || conf.pansList || [];
      for (var i = 0; i < ps.length; i++) {
        var p = ps[i];
        if (p && (p.key === k || p.type === k || p.panKey === k) && p.cookie) return String(p.cookie);
      }
    } catch (e) { }
    return "";
  }
  function panCookie(P, conf) {
    for (var i = 0; i < P.ck.length; i++) {
      var c = cookieOf(conf, P.ck[i]);
      if (c) return c;
    }
    return "";
  }

  function shareOf(u) {
    var m = /\/s\/([0-9a-zA-Z]+)/.exec(String(u || ""));
    if (!m) return null;
    var pwd = "";
    var p = /[?&](?:pwd|password|code)=([0-9a-zA-Z]{4})/.exec(String(u || ""));
    if (p) pwd = p[1];
    return { id: m[1], pwd: pwd };
  }

  var PAN_TOKEN = {}, PAN_DIRECT = {}, PAN_ERR = {};

  function panHdr(P, cookie) {
    return {
      "User-Agent": P.ua,
      "Referer": P.ref,
      "Origin": P.ref,
      "Accept": "application/json, text/plain, */*",
      "Cookie": cookie
    };
  }

  function errOf(d) {
    if (!d) return "empty";
    if (d.status && d.status !== 200) return "HTTP " + d.status;
    if (d.code && d.code !== 0) return "code " + d.code + (d.message ? " " + d.message : "");
    return "";
  }

  /* 1) 分享 → stoken */
  async function panToken(P, sh, cookie, force) {
    var ck = P.k + "|" + sh.id + "|" + sh.pwd;
    var hit = PAN_TOKEN[ck];
    if (!force && hit && Date.now() - hit.at < 20 * 60 * 1000) return hit.stoken;
    var r = await postJson(P.base + "share/sharepage/token?" + P.q,
      { pwd_id: sh.id, passcode: sh.pwd || "" }, panHdr(P, cookie), { timeout: 15000 });
    var d = jsonOf(r);
    var err = errOf(d);
    if (err) { PAN_ERR[ck] = err; return ""; }
    var st = d && d.data && d.data.stoken;
    if (!st) { PAN_ERR[ck] = "no stoken"; return ""; }
    delete PAN_ERR[ck];
    PAN_TOKEN[ck] = { stoken: st, at: Date.now() };
    return st;
  }

  /* 2) stoken → 视频文件（递归目录） */
  async function panList(P, sh, stoken, cookie, pdir, depth) {
    depth = depth || 0;
    if (depth > 5) return [];
    var u = P.base + "share/sharepage/detail?" + P.q +
      "&pwd_id=" + encodeURIComponent(sh.id) +
      "&stoken=" + encodeURIComponent(stoken) +
      "&pdir_fid=" + encodeURIComponent(pdir || "0") +
      "&force=0&_page=1&_size=200&_sort=file_type:asc,file_name:asc";
    var r = await rawReq(u, { headers: panHdr(P, cookie), timeout: 20000 });
    var d = jsonOf(r);
    if (errOf(d)) return [];
    var list = (d.data && d.data.list) || [];
    var out = [], subs = [];
    for (var i = 0; i < list.length; i++) {
      var x = list[i];
      if (!x) continue;
      if (x.dir === true) { subs.push(x.fid); continue; }
      if (!QK_VIDEO.test(String(x.file_name || ""))) continue;
      out.push({
        fid: String(x.fid || ""),
        token: String(x.share_fid_token || x.fid_token || x.file_token || x.share_file_token || x.obj_token || ""),
        name: String(x.file_name || ""),
        size: Number(x.size || x.file_size || 0) || 0
      });
    }
    if (subs.length && out.length < 200) {
      var arrs = await Promise.all(subs.slice(0, 12).map(function (fid) {
        return panList(P, sh, stoken, cookie, fid, depth + 1).catch(function () { return []; });
      }));
      arrs.forEach(function (a) { out = out.concat(a); });
    }
    return out;
  }

  /* 3) 文件 → CDN 直链 */
  async function panDirect(P, sh, stoken, cookie, file) {
    var ck = P.k + "|" + sh.id + "|" + file.fid;
    var hit = PAN_DIRECT[ck];
    if (hit && Date.now() - hit.at < 8 * 60 * 1000) return hit;
    var body = {
      fids: [file.fid],
      fids_token: [file.token],
      pwd_id: sh.id,
      stoken: stoken,
      speedup_session: ""
    };
    var r = await postJson(P.base + "file/download?" + P.q, body, panHdr(P, cookie), { timeout: 20000 });
    var d = jsonOf(r);
    var err = errOf(d);
    if (err) { PAN_ERR[ck] = err; return null; }
    var it = d && d.data && d.data[0];
    var u = it && it.download_url;
    if (!u) { PAN_ERR[ck] = "no download_url"; return null; }
    delete PAN_ERR[ck];
    var rec = {
      url: String(u),
      headers: { "User-Agent": P.ua, Referer: P.ref },
      size: file.size || 0,
      at: Date.now()
    };
    PAN_DIRECT[ck] = rec;
    return rec;
  }

  /* 探直链：跟随 302，确认 Range / 总长 */
  function probeDirect(u, hd, depth) {
    depth = depth || 0;
    return rawReq(u, {
      headers: Object.assign({}, hd, { Range: "bytes=0-1", "Accept-Encoding": "identity" }),
      stream: true, timeout: 6000, watchdog: 8000
    }).then(function (res) {
      var st = res.statusCode, h = res.headers || {};
      try { res.destroy(); } catch (e) { }
      if (st >= 300 && st < 400 && h.location && depth < 4) {
        var nx = String(h.location);
        if (nx.charAt(0) === "/") {
          var p = parseUrl(u);
          if (p) nx = p.protocol + "//" + p.host + nx;
        }
        return probeDirect(nx, hd, depth + 1);
      }
      var m = /\/(\d+)\s*$/.exec(String(h["content-range"] || ""));
      return {
        url: u,
        ctype: String(h["content-type"] || ""),
        size: m ? Number(m[1]) : 0,
        range: !!m,
        status: st
      };
    }).catch(function () { return { url: u, ctype: "", size: 0, range: false, status: 0 }; });
  }
  /* ================= 多线程分块代理 ================= */

  var ITEMS = {}, SEQ = 0;
  var RL_POOL_MAX = 16, RL_LAST_THROTTLE = 0;

  /* 被限流后并发会临时降下来，闲 60 秒再慢慢加回去 */
  function poolTick() {
    if (RL_POOL < RL_POOL_MAX && Date.now() - RL_LAST_THROTTLE > 60000) RL_POOL++;
  }

  function sniff(head, url) {
    var shift = 0, ctype = "";
    if (head && head.length) {
      var i = head.indexOf(Buffer.from([0x1a, 0x45, 0xdf, 0xa3]));
      var j = head.indexOf(Buffer.from("ftyp"));
      if (j >= 4) {
        var bs = head.readUInt32BE(j - 4);
        if (bs < 8 || bs > 64) j = -1;
      }
      if (i >= 0 && i < 48 && (j < 4 || i <= j - 4)) { shift = i; ctype = "video/x-matroska"; }
      else if (j >= 4) { shift = j - 4; ctype = head.slice(j + 4, j + 6).toString() === "qt" ? "video/quicktime" : "video/mp4"; }
      else if (head.slice(0, 3).toString() === "FLV") ctype = "video/x-flv";
      else if (head.length >= 377 && head[0] === 0x47 && head[188] === 0x47) ctype = "video/mp2t";
    }
    if (!ctype) {
      var ext = (String(url || "").split("?")[0].match(/\.([a-z0-9]{2,4})$/i) || [, ""])[1].toLowerCase();
      ctype = { mp4: "video/mp4", mkv: "video/x-matroska", flv: "video/x-flv", ts: "video/mp2t", avi: "video/x-msvideo", mov: "video/quicktime" }[ext] || "video/mp4";
    }
    return { shift: shift, ctype: ctype };
  }

  function rlLog() {
    if (process.env.RL_DEBUG) process.stderr.write("[xkvip] " + Array.prototype.join.call(arguments, " ") + "\n");
  }

  function fetchRange(it, start, end, tries) {
    tries = tries || 4;
    var want = end - start + 1;
    var shift = it.shift || 0;
    function attempt(n) {
      if (n > tries) return Promise.resolve(Buffer.alloc(0));
      return rawReq(it.url, {
        headers: Object.assign({}, it.headers, {
          Range: "bytes=" + (start + shift) + "-" + (end + shift),
          "Accept-Encoding": "identity"
        }),
        stream: true, timeout: 20000, watchdog: 9000, noAgent: true
      }).then(function (res) {
        var st = res.statusCode;
        if (st === 429 || st === 403) {
          /* 被限并发：整体降速，让出连接 */
          RL_LAST_THROTTLE = Date.now();
          if (RL_POOL > 6) RL_POOL = Math.max(6, RL_POOL - 2);
          try { res.destroy(); } catch (e) { }
          return new Promise(function (r2) { setTimeout(r2, 180 * n); }).then(function () { return attempt(n + 1); });
        }
        if (st !== 200 && st !== 206) {
          try { res.destroy(); } catch (e) { }
          if (it.refresh && n === 1) {
            return Promise.resolve(it.refresh()).then(function (nx) {
              if (nx && nx.url) { it.url = nx.url; it.headers = nx.headers || it.headers; }
              return attempt(n + 1);
            });
          }
          return attempt(n + 1);
        }
        return new Promise(function (resolve) {
          var buf = [], got = 0, done = false, idle = null;
          function arm() {
            if (idle) clearTimeout(idle);
            idle = setTimeout(function () { try { res.destroy(); } catch (e) { } }, 12000);
          }
          function fin(b) { if (done) return; done = true; if (idle) clearTimeout(idle); resolve(b); }
          arm();
          res.on("data", function (c) {
            if (done) return;
            arm();
            buf.push(c); got += c.length;
            if (got >= want) {
              fin(Buffer.concat(buf).slice(0, want));
              try { res.destroy(); } catch (e) { }
            }
          });
          res.on("end", function () { fin(got >= want ? Buffer.concat(buf).slice(0, want) : Buffer.alloc(0)); });
          res.on("error", function () { fin(Buffer.alloc(0)); });
          res.on("close", function () { fin(got >= want ? Buffer.concat(buf).slice(0, want) : Buffer.alloc(0)); });
        }).then(function (b) {
          if (b.length === want) return b;
          return new Promise(function (r3) { setTimeout(r3, 120 * n); }).then(function () { return attempt(n + 1); });
        });
      }).catch(function (e) {
        rlLog("range err", start, end, n, e && e.message);
        return new Promise(function (r4) { setTimeout(r4, 120 * n); }).then(function () { return attempt(n + 1); });
      });
    }
    return attempt(1);
  }

  function cachePut(it, p, buf) {
    if (!it.rlCache) { it.rlCache = {}; it.rlCacheBytes = 0; }
    if (!buf || !buf.length || it.rlCache[p]) return;
    it.rlCache[p] = buf;
    if (p + buf.length > (it.rlMax || 0)) it.rlMax = p + buf.length;
    it.rlCacheBytes += buf.length;
    while (it.rlCacheBytes > RL_CACHE) {
      var ks = Object.keys(it.rlCache);
      if (!ks.length) break;
      it.rlCacheBytes -= it.rlCache[ks[0]].length;
      delete it.rlCache[ks[0]];
    }
  }

  function rlChunk(off, limit) {
    var sz = off < RL_HEAD * 2 ? Math.min(RL_CHUNK, 256 * 1024) : RL_CHUNK;
    return (limit && sz > limit) ? limit : sz;
  }

  function headFast(it) {
    if (it.rlHp) return it.rlHp;
    it.rlHp = (function () {
      var tasks = [];
      for (var i = 0; i < RL_HW; i++) {
        var s0 = i * RL_HS, e0 = Math.min(s0 + RL_HS, RL_HEAD) - 1;
        if (s0 > e0) break;
        tasks.push(fetchRange(it, s0, e0, 2));
      }
      return Promise.all(tasks).then(function (arrs) {
        for (var k = 0; k < arrs.length; k++) if (!arrs[k] || !arrs[k].length) return fetchRange(it, 0, RL_HEAD - 1, 4);
        return Buffer.concat(arrs);
      }).catch(function () {
        return fetchRange(it, 0, RL_HEAD - 1, 4);
      }).then(function (b) {
        if (b && b.length) cachePut(it, 0, b);
        return b;
      });
    })();
    return it.rlHp;
  }

  function preload(it) {
    var total = (it.size || 0) - (it.shift || 0);
    if (total <= 0 || it.rlPre) return;
    if (!it.rlBusy) it.rlBusy = {};
    var top = Math.min(total, RL_PRELOAD), p = RL_HEAD, inflight = 0, stop = false;
    it.rlPre = { stop: function () { stop = true; } };
    function step() {
      while (!stop && inflight < RL_PW && p < top) {
        var sz = rlChunk(p, top - p), b = Math.min(p + sz - 1, top - 1);
        if (it.rlCache && it.rlCache[p]) { p = b + 1; continue; }
        (function (pp, bb) {
          inflight++;
          var pr = fetchRange(it, pp, bb, 2).then(function (x) {
            cachePut(it, pp, x); if (it.rlBusy) delete it.rlBusy[pp];
            inflight--; step(); return x;
          }).catch(function () {
            if (it.rlBusy) delete it.rlBusy[pp]; inflight--; step(); return Buffer.alloc(0);
          });
          it.rlBusy[pp] = { b: bb, pr: pr };
        })(p, b);
        p = b + 1;
      }
    }
    headFast(it).then(function () { setTimeout(function () { if (!stop) step(); }, 60); })
      .catch(function () { setTimeout(function () { if (!stop) step(); }, 60); });
  }

  function handleStream(req, res, it) {
    var LG = rlLog;
    poolTick();
    function head0() {
      var c = it.rlCache && it.rlCache[0];
      if (c && c.length >= Math.min(RL_HEAD, it.size)) return Promise.resolve(c);
      return headFast(it);
    }
    head0().then(function (head) {
      var sn = sniff(head, it.url);
      it.shift = sn.shift; it.ctype = sn.ctype;
      var total = (it.size || 0) - (it.shift || 0);
      if (!total || total <= 0) { res.statusCode = 502; return res.end(); }
      var hdr = (req.headers && req.headers.range) || "";
      var start = 0, end = total - 1, m = /bytes=(\d*)-(\d*)/.exec(hdr);
      if (m) {
        if (m[1] === "" && m[2]) { start = Math.max(0, total - Number(m[2])); end = total - 1; }
        else { start = Number(m[1] || 0); end = m[2] ? Number(m[2]) : total - 1; }
      }
      if (start >= total) {
        res.statusCode = 416;
        res.setHeader("Content-Range", "bytes */" + total);
        res.setHeader("Content-Length", "0");
        return res.end();
      }
      if (end >= total) end = total - 1;
      if (end < start) end = start;
      res.statusCode = hdr ? 206 : 200;
      res.setHeader("Content-Type", it.ctype || "video/mp4");
      res.setHeader("Accept-Ranges", "bytes");
      res.setHeader("Content-Length", String(end - start + 1));
      if (hdr) res.setHeader("Content-Range", "bytes " + start + "-" + end + "/" + total);
      res.setHeader("Cache-Control", "no-store");
      res.setHeader("Connection", "close");
      if (req.method === "HEAD") { res.end(); return; }

      var closed = false;
      res.on("close", function () { closed = true; });

      var pending = {}, order = [], nextPos = start, outPos = start;

      function submit(p) {
        var sz = rlChunk(p - start, end - p + 1), b = p + sz - 1;
        var hit = it.rlCache && it.rlCache[p];
        if (hit && hit.length === b - p + 1) { pending[p] = Promise.resolve(hit); return b + 1; }
        var busy = it.rlBusy && it.rlBusy[p];
        if (busy && busy.b === b) { pending[p] = busy.pr; return b + 1; }
        var pr = fetchRange(it, p, b, 4).then(function (x) {
          cachePut(it, p, x);
          if (it.rlBusy) delete it.rlBusy[p];
          return x;
        });
        if (!it.rlBusy) it.rlBusy = {};
        it.rlBusy[p] = { b: b, pr: pr };
        pending[p] = pr;
        return b + 1;
      }

      var first = (start === 0 && head && head.length) ? head.slice(0, Math.min(it.shift + RL_HEAD, end - start + 1)) : null;
      if (first) {
        outPos = start + first.length;
        nextPos = start + first.length;
        res.write(first);
      }
      while (nextPos <= end && order.length < RL_POOL) {
        order.push(nextPos);
        nextPos = submit(nextPos);
      }

      var retryN = {};
      function pump() {
        if (closed) return;
        var p = order.shift();
        if (p === undefined) return;
        var pr = pending[p];
        if (!pr) return res.end();
        pr.then(function (buf) {
          delete pending[p];
          if (closed) return;
          if (!buf || !buf.length) {
            var n = (retryN[p] || 0) + 1;
            if (n <= 8) {
              retryN[p] = n;
              order.unshift(p);
              var sz2 = rlChunk(p - start, end - p + 1);
              pending[p] = fetchRange(it, p, p + sz2 - 1, 4).then(function (x) {
                cachePut(it, p, x);
                if (it.rlBusy) delete it.rlBusy[p];
                return x;
              });
              return setTimeout(pump, 150 * n);
            }
            try { res.destroy(); } catch (e) { }
            return;
          }
          var ok = res.write(buf, function () {
            if (closed) return;
            outPos += buf.length;
            if (nextPos <= end) { order.push(nextPos); nextPos = submit(nextPos); }
            if (outPos > end) return res.end();
            pump();
          });
          if (!ok) res.once("drain", function () { /* write 回调会继续 pump */ });
        }).catch(function (e2) {
          LG("pump err", e2 && e2.message);
          try { res.destroy(); } catch (e) { }
        });
      }
      pump();
    }).catch(function (e3) {
      LG("stream err", e3 && e3.message);
      try { res.statusCode = 502; res.end(); } catch (e) { }
    });
  }

  function mkSid(it) {
    SEQ++;
    var sid = ((SEQ * 2246822519) >>> 0).toString(16) + ((Date.now() & 0xffff) >>> 0).toString(16);
    ITEMS[sid] = it;
    var ks = Object.keys(ITEMS);
    if (ks.length > 12) ks.slice(0, ks.length - 12).forEach(function (k) { delete ITEMS[k]; });
    return sid;
  }

  function relayUrl(req, it) {
    var sid = mkSid(it);
    var host = (req && req.headers && (req.headers.host || req.headers.Host)) || "";
    var root = host ? (/^https?:\/\//i.test(host) ? host : "http://" + host) : "";
    if (!root) return "";
    preload(it);
    return root + "/spider/" + KEY + "/3/proxy/raw/" + sid;
  }

  /* ================= 播放线路 ================= */

  function b64(o) {
    try { return Buffer.from(JSON.stringify(o), "utf8").toString("base64url"); } catch (e) { return ""; }
  }
  function unb64(s) {
    try { return JSON.parse(Buffer.from(String(s || ""), "base64url").toString("utf8")); } catch (e) { return null; }
  }

  function epNo(name, idx) {
    var s = String(name || "");
    var m = /[Ss](\d{1,2})[Ee](\d{1,3})/.exec(s);
    if (m) return "S" + m[1] + "E" + m[2];
    m = /第\s*(\d{1,4})\s*[集话話期]/.exec(s);
    if (m) return "第" + m[1] + "集";
    m = /(?:^|[^A-Za-z])[Ee][Pp]?(\d{1,4})(?:[^0-9]|$)/.exec(s);
    if (m) return "第" + Number(m[1]) + "集";
    return "第" + (idx + 1) + "集";
  }

  /* 一条分享 → 该网盘下的全部视频线路 */
  async function buildLines(P, shareUrl, cookie) {
    var sh = shareOf(shareUrl);
    if (!sh) return [];
    var st = await panToken(P, sh, cookie);
    if (!st) return [];
    var files = await panList(P, sh, st, cookie, "0", 0);
    if (!files.length) return [];
    files.sort(function (a, b) {
      return String(a.name).localeCompare(String(b.name), "zh-CN", { numeric: true });
    });
    return files.map(function (f, i) {
      return {
        name: epNo(f.name, i),
        id: P.k + ":" + b64({ d: sh.id, p: sh.pwd, f: f.fid, t: f.token, s: f.size, n: f.name })
      };
    });
  }

  /* 详情：页面里的每条网盘链接 → 一路 from */
  async function detailLogin(raw, e, req) {
    var id = String(raw || "").trim();
    if (!id) return { list: [] };
    var conf = confOf(e, req);

    /* 直接丢分享链接进来也能播 */
    var hit0 = null;
    PAN_RES.forEach(function (p) { if (p[1].test(id)) hit0 = p[0]; });
    if (hit0) {
      var only = {};
      only[hit0] = [id];
      return { list: [await vodFromPans(id, id, "", "", only, conf, req)] };
    }

    var html = await getHtml("/movie/" + encodeURIComponent(id) + "/");
    if (html && html.indexOf(id) < 0) html = "";
    if (!html) return { list: [xkHint(id, "没找到这部片（详情页打不开或已下架）", null, req, KEY)] };
    var meta = detailMeta(html);
    var pans = pansOf(html);
    if (!pans || !Object.keys(pans).length) {
      return { list: [xkHint(id, "本站标记「待补资源」，还没补上网盘", meta, req, KEY)] };
    }
    var pic = meta.pic ? xkPic(meta.pic, req, KEY, 92) : "";
    return { list: [await vodFromPans(id, meta.name, pic, meta.content, pans, conf, req)] };
  }

  async function vodFromPans(vodId, title, pic, content, pans, conf, req) {
    var froms = [], urls = [], notes = [];
    var kinds = [
      { p: PANS.quark, links: pans["夸克"] || [] },
      { p: PANS.uc, links: pans["UC"] || [] }
    ];

    for (var i = 0; i < kinds.length; i++) {
      var k = kinds[i];
      if (!k.links.length) continue;
      var cookie = panCookie(k.p, conf);
      if (!cookie) { notes.push(k.p.name + " 未填 cookie"); continue; }
      for (var j = 0; j < k.links.length && j < 3; j++) {
        var lines = [];
        try { lines = await buildLines(k.p, k.links[j], cookie); } catch (e) { lines = []; }
        if (!lines.length) {
          var sh = shareOf(k.links[j]);
          var ck = k.p.k + "|" + (sh ? sh.id : "") + "|" + (sh ? sh.pwd : "");
          notes.push(k.p.name + " 第" + (j + 1) + "条无可用文件" + (ck && PAN_ERR[ck] ? "(" + PAN_ERR[ck] + ")" : ""));
          continue;
        }
        froms.push(k.p.name + (j ? " " + (j + 1) : "") + " " + k.p.tag);
        urls.push(lines.map(function (x) { return x.name + "$" + x.id; }).join("#"));
      }
    }

    if (!froms.length) {
      froms.push("提示");
      urls.push("没取到可播直链" + (notes.length ? "：" + notes.slice(0, 3).join("；") : "") + "。请在猫源【配置】里填好夸克 / UC cookie，或换一个资源$none");
    }

    return {
      vod_id: String(vodId),
      vod_name: String(title || vodId),
      vod_pic: pic || "",
      vod_content: content || "",
      vod_play_from: froms.join("$$$"),
      vod_play_url: urls.join("$$$")
    };
  }

  /* ---------- 代理项：URL 自包含（玩偶式），进程重启也能继续播 ---------- */

  var ITEM_CACHE = {};

  function sigOf(o) {
    return b64({ k: o.k, d: o.d, p: o.p || "", f: o.f, t: o.t, s: o.s || 0, n: o.n || "" });
  }

  async function ensureItem(o, conf, req) {
    var P = PANS[o.k];
    if (!P) return null;
    var cookie = panCookie(P, conf);
    if (!cookie) return null;
    var ck = P.k + "|" + o.d + "|" + o.f;
    var hit = ITEM_CACHE[ck];
    if (hit && Date.now() - hit.at < 7 * 60 * 1000) return hit.it;

    var sh = { id: o.d, pwd: o.p || "" };
    var st = await panToken(P, sh, cookie);
    if (!st) return null;
    var direct = await panDirect(P, sh, st, cookie, { fid: o.f, token: o.t, size: o.s, name: o.n });
    if (!direct) return null;

    var pb = await probeDirect(direct.url, direct.headers);
    var size = pb.size || direct.size || Number(o.s) || 0;
    var it = {
      url: pb.url || direct.url,
      headers: direct.headers,
      size: size,
      refresh: function () {
        delete PAN_DIRECT[P.k + "|" + sh.id + "|" + o.f];
        delete ITEM_CACHE[ck];
        return panToken(P, sh, cookie, true).then(function (st2) {
          if (!st2) return null;
          return panDirect(P, sh, st2, cookie, { fid: o.f, token: o.t, size: size });
        }).then(function (nd) {
          if (!nd) return null;
          return probeDirect(nd.url, nd.headers).then(function (p2) {
            return { url: p2.url || nd.url, headers: nd.headers, size: p2.size || size };
          });
        }).catch(function () { return null; });
      }
    };
    ITEM_CACHE[ck] = { it: it, at: Date.now() };
    return it;
  }

  /* 播放：只返回代理地址，真正取链推迟到第一个 Range 请求 */
  function playOf(pid, e, req) {
    var s = String(pid || "");
    if (s.indexOf("quark:") !== 0 && s.indexOf("uc:") !== 0) {
      if (/^https?:\/\//i.test(s)) return { parse: 0, url: s };
      return { parse: 0, url: "", msg: "线路无效" };
    }
    var kind = s.slice(0, s.indexOf(":"));
    var P = PANS[kind];
    if (!P) return { parse: 0, url: "", msg: "不支持的网盘" };
    var o = unb64(s.slice(kind.length + 1));
    if (!o || !o.d || !o.f) return { parse: 0, url: "", msg: "线路参数损坏" };

    var conf = confOf(e, req);
    if (!panCookie(P, conf)) return { parse: 0, url: "", msg: "没填" + P.name + " cookie：请在猫源【配置】里登录" + P.name };

    var host = (req && req.headers && (req.headers.host || req.headers.Host)) || "";
    var root = host ? (/^https?:\/\//i.test(host) ? host : "http://" + host) : "";
    if (!root) return { parse: 0, url: "", msg: "取不到本机地址" };

    o.k = kind;
    return {
      parse: 0,
      url: root + "/spider/" + KEY + "/3/proxy/raw/" + sigOf(o),
      header: { "User-Agent": P.ua }
    };
  }
  /* ================= 星光影库抓取（详情页 / 网盘链接） ================= */

  function getHtml(path) {
    return rawReq(XK_BASE + path, {
      headers: {
        "User-Agent": XK_UA,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Encoding": "gzip",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": XK_BASE + "/"
      },
      timeout: 25000
    }).then(function (r) { return r && r.status === 200 ? r.text : ""; })
      .catch(function () { return ""; });
  }

  function detailMeta(html) {
    var meta = { name: "", pic: "", content: "" };
    var h1 = /<h1[^>]*>([\s\S]{0,200}?)<\/h1>/.exec(html);
    if (h1) meta.name = xkText(h1[1]);
    var ti = /<title>《?([^》<(]{1,60})》?\s*\((\d{4})\)/.exec(html);
    if (!meta.name && ti) meta.name = ti[1];
    var og = /property="og:image"\s+content="([^"]+)"/.exec(html) ||
      /content="([^"]+)"\s+property="og:image"/.exec(html);
    if (og) meta.pic = xkUnesc(og[1]);
    var dm = /剧情简介[\s\S]{0,160}?<p[^>]*>([\s\S]{0,1200}?)<\/p>/.exec(html);
    if (dm) meta.content = xkText(dm[1]);
    if (!meta.content) {
      var dd = /<meta name="description" content="([^"]*)"/.exec(html);
      if (dd) meta.content = xkUnesc(dd[1]).replace(/^[\s\S]{0,40}?剧情简介[:：]/, "").slice(0, 500);
    }
    if (meta.content.length > 420) meta.content = meta.content.slice(0, 418) + "…";
    return meta;
  }

  function pansOf(html) {
    var found = {}, txt = String(html || "").replace(/&amp;/g, "&").replace(/\\\//g, "/");
    var re = /data-clipboard-text="([^"]+)"/gi, m;
    while ((m = re.exec(txt)) !== null) {
      var u = m[1].trim();
      PAN_RES.forEach(function (p) {
        if (p[1].test(u)) {
          if (!found[p[0]]) found[p[0]] = [];
          if (found[p[0]].indexOf(u) < 0) found[p[0]].push(u);
        }
      });
    }
    if (!Object.keys(found).length) {
      PAN_RES.forEach(function (p) {
        var r = new RegExp(p[1].source, "gi"), mm;
        while ((mm = r.exec(txt)) !== null) {
          var u2 = mm[0].replace(/[\\"'.,;)]+$/, "");
          if (!found[p[0]]) found[p[0]] = [];
          if (found[p[0]].indexOf(u2) < 0) found[p[0]].push(u2);
        }
      });
    }
    Object.keys(found).forEach(function (k) { found[k] = found[k].slice(0, 3); });
    return found;
  }

  /* ================= 站点 ================= */

  function makeSite() {
    var st = { cat: null, catAt: 0, catBusy: null, detail: {} };

    function catalog() {
      if (st.cat && Date.now() - st.catAt < 20 * 60 * 1000) return Promise.resolve(st.cat);
      if (st.catBusy) return st.catBusy;
      st.catBusy = getHtml("/explore/").then(function (html) {
        var list = [], seen = {}, raw = xkParse(html);
        raw.forEach(function (x) { if (!seen[x.id]) { seen[x.id] = 1; list.push(x); } });
        if (list.length) { st.cat = list; st.catAt = Date.now(); }
        st.catBusy = null;
        return st.cat || [];
      }).catch(function () { st.catBusy = null; return st.cat || []; });
      if (st.cat) return Promise.resolve(st.cat);
      return st.catBusy;
    }

    function catOf(tid) {
      for (var i = 0; i < XK_CATS.length; i++) if (XK_CATS[i][0] === tid) return XK_CATS[i];
      return null;
    }

    function filterList(all, tid, ext) {
      var cat = catOf(tid), list = all;
      if (cat) list = list.filter(function (x) { return x.cat === cat[2]; });
      if (ext["class"]) list = list.filter(function (x) { return x.tags.indexOf(ext["class"]) >= 0; });
      if (ext.area) list = list.filter(function (x) { return x.area === ext.area; });
      if (ext.year) list = list.filter(function (x) { return String(x.year) === String(ext.year); });
      return xkSort(list, ext.by);
    }

    var site = {
      meta: { key: KEY, name: TITLE, type: 3, searchable: 1, quickSearch: 1, filterable: 1 },
      api: async function (e) {
        e.post("/init", async function (req, reply) {
          st.cat = null; st.catAt = 0; st.catBusy = null; st.detail = {};
          catalog();
          xkProbePic().catch(function () { });
          return reply.send({});
        });

        e.post("/home", async function (req, reply) {
          var classes = XK_CATS.map(function (c) { return { type_id: c[0], type_name: c[1] }; });
          var all = await catalog();
          return reply.send({ class: classes, filters: xkFilters(), list: xkVods(filterList(all, "movie", {}), 60, req, KEY) });
        });

        e.post("/homeVod", async function (req, reply) {
          var all = await catalog();
          return reply.send({ list: xkVods(filterList(all, "movie", {}), 60, req, KEY) });
        });

        e.post("/category", async function (req, reply) {
          var b = (req && req.body) || {};
          var tid = String(b.id || b.tid || b.type_id || "movie");
          var page = Math.max(1, parseInt(b.page || 1, 10) || 1);
          var ext = b.extend || b.filters || b.ext || {};
          if (typeof ext === "string") { try { ext = JSON.parse(ext); } catch (e1) { ext = {}; } }
          if (!ext || typeof ext !== "object") ext = {};
          var all = await catalog();
          if (!all.length) return reply.send({ page: page, pagecount: 1, limit: 0, total: 0, list: [] });
          var list = filterList(all, tid, ext);
          var total = list.length;
          var pages = Math.max(1, Math.ceil(total / XK_PAGE));
          if (page > pages) page = pages;
          var slice = list.slice((page - 1) * XK_PAGE, page * XK_PAGE);
          return reply.send({ page: page, pagecount: pages, limit: slice.length, total: total, list: xkVods(slice, XK_PAGE, req, KEY) });
        });

        e.post("/search", async function (req, reply) {
          var b = (req && req.body) || {};
          var wd = String(b.wd || b.key || b.word || "").trim();
          var page = Math.max(1, parseInt(b.page || 1, 10) || 1);
          if (!wd) return reply.send({ page: 1, pagecount: 1, limit: 0, total: 0, list: [] });
          var all = await catalog();
          var q = wd.toLowerCase(), hit = [], fuzzy = [];
          all.forEach(function (x) {
            var n = x.name.toLowerCase();
            if (n.indexOf(q) >= 0) hit.push(x);
            else if (x.tags.some(function (t) { return String(t).toLowerCase().indexOf(q) >= 0; })) fuzzy.push(x);
          });
          var list = xkSort(hit, "").concat(xkSort(fuzzy, ""));
          var total = list.length;
          var pages = Math.max(1, Math.ceil(total / XK_PAGE));
          if (page > pages) page = pages;
          var slice = list.slice((page - 1) * XK_PAGE, page * XK_PAGE);
          return reply.send({ page: page, pagecount: pages, limit: slice.length, total: total, list: xkVods(slice, XK_PAGE, req, KEY) });
        });

        e.post("/detail", async function (req, reply) {
          var b = (req && req.body) || {};
          var raw = Array.isArray(b.id) ? b.id[0] : b.id;
          var key0 = String(raw || "");
          var hit = st.detail[key0];
          if (hit && Date.now() - hit[1] < hit[2]) return reply.send(hit[0]);
          var out;
          try {
            out = await detailLogin(raw, e, req);
          } catch (e2) {
            out = { list: [{ vod_id: key0, vod_name: key0, vod_play_from: "提示", vod_play_url: "解析异常：" + (e2 && e2.message) + "$none" }] };
          }
          var v = (out.list || [])[0] || {};
          var ok = v.vod_play_from && v.vod_play_from !== "提示";
          st.detail[key0] = [out, Date.now(), ok ? 10 * 60 * 1000 : 60 * 1000];
          var ks = Object.keys(st.detail);
          if (ks.length > 60) ks.slice(0, 30).forEach(function (k) { delete st.detail[k]; });
          return reply.send(out);
        });

        e.post("/play", async function (req, reply) {
          var b = Object.assign({}, (req && req.query) || {}, (req && req.body) || {});
          try {
            return reply.send(await playOf(b.id || b.url, e, req));
          } catch (e3) {
            return reply.send({ parse: 0, url: "", msg: "播放异常：" + (e3 && e3.message) });
          }
        });

        /* 代理地址自带分享/文件参数：重启后照样能播，seek 命中同一份缓存 */
        e.get("/proxy/raw/:sig", async function (req, reply) {
          var o = unb64(req.params.sig);
          if (!o || !o.k || !o.f) return reply.code(404).send({ error: "bad sig" });
          var it = null;
          try { it = await ensureItem(o, confOf(e, req), req); } catch (e1) { it = null; }
          if (!it) return reply.code(502).send({ error: "source unavailable" });
          reply.hijack();
          handleStream(req.raw, reply.raw, it);
          return reply;
        });

        e.get("/proxy/probe/:sig", async function (req, reply) {
          var o = unb64(req.params.sig);
          if (!o || !o.k || !o.f) return reply.code(404).send({ error: "bad sig" });
          var it = null;
          try { it = await ensureItem(o, confOf(e, req), req); } catch (e2) { it = null; }
          if (!it) return reply.code(502).send({ error: "source unavailable" });
          fetchRange(it, 0, 1023, 2).then(function (x) {
            reply.code(x && x.length ? 200 : 502).header("Content-Type", "application/octet-stream").send(x || Buffer.alloc(0));
          });
        });

        e.get("/pic/:sig", async function (req, reply) {
          var sig = String((req.params && req.params.sig) || "");
          var wq = Math.max(120, Math.min(600, parseInt((req.query && req.query.w) || 240, 10) || 240));
          var im = await xkPicGet(sig, wq);
          if (!im) {
            var u = "";
            try { u = Buffer.from(sig, "base64url").toString("utf8"); } catch (e4) { u = ""; }
            if (!/^https?:\/\//i.test(u)) return reply.code(404).send({ error: "bad pic" });
            return reply.redirect(xkCdn(u, wq));
          }
          return reply.header("Content-Type", im.ct).header("Cache-Control", "public, max-age=604800")
            .header("Content-Length", im.buf.length).send(im.buf);
        });
      },
      check: function () { return true; }
    };
    return site;
  }

  var SITE = makeSite();
  SITE.__debug = { rawReq: rawReq, handleStream: handleStream, fetchRange: fetchRange, ITEMS: ITEMS, mkSid: mkSid, RL: function () { return { pool: RL_POOL, chunk: RL_CHUNK, head: RL_HEAD }; } };
  return SITE;

})();
