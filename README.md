# map_wmts

`map_wmts` 是一个本地 WMTS 代理，用于把一个或多个在线瓦片模板封装成 WMTS 服务，供 ArcMap / QGIS 等桌面 GIS 直接接入。

## 主要功能

1. 提供本地 WMTS 服务入口：
   - `http://127.0.0.1:<port>/wmts?SERVICE=WMTS&REQUEST=GetCapabilities`
2. 支持单图层与多图层配置。
3. 支持模板占位符：`{z}`、`{x}`、`{y}`、`{y_inv}`（兼容 `{-y}`）。
4. 支持内存缓存与磁盘缓存（SQLite，异常时可回退文件缓存）。
5. 支持上游连接池参数调优。
6. 支持兼容模式 WMTS 请求处理（降低 ArcMap 探测请求导致的失败概率）。

> 可以在arcmap中关闭自带缓存，以加快速度。图层右键-cache。

## 使用方式

### 1) 单图层启动

```powershell
Set-Location .\map_wmts
.\start_wmts_proxy.ps1 -UpstreamTileUrlTemplate 'https://.../vt/lyrs=s&x={x}&y={y}&z={z}'
```

### 2) 预设多图层启动（推荐）

```powershell
Set-Location .\map_wmts
$env:TIANDITU_TK = '你的天地图tk'
.\start_wmts_proxy.ps1 -GoogleMultiLayers
```

重要：start_wmts_proxy.ps1中未包含图层地址，需自行查找替换。不设置天地图tk则自动跳过。

### 3) 自定义多图层启动

```powershell
$layers = @(
  @{ id='sat'; title='Google Satellite'; template='https://...&x={x}&y={y}&z={z}'; format='image/jpeg' },
  @{ id='tx';  title='Tencent Vector';  template='http://...z={z}&x={x}&y={y_inv}&type=vector&style=0'; format='image/png' }
) | ConvertTo-Json -Compress

.\start_wmts_proxy.ps1 -WmtsLayersJson $layers
```

### 4) ArcMap 接入

在 ArcMap 中添加 WMTS 服务：

```text
http://127.0.0.1:8787/wmts?SERVICE=WMTS&REQUEST=GetCapabilities
```

### 5) 运行状态检查

```text
http://127.0.0.1:8787/health
http://127.0.0.1:8787/stats
```

## 业务逻辑说明

1. 请求解析：
   - 处理 `GetCapabilities` 与 `GetTile`。
   - 兼容模式下对部分非标准请求进行容错。
2. 图层路由：
   - 根据 `LAYER` 选择图层模板。
   - 按 `z/x/y/y_inv` 生成上游请求 URL。
3. 缓存逻辑：
   - 先查内存缓存，再查磁盘缓存。
   - 未命中时回源。
4. 负缓存：
   - 对上游 `404` 可返回透明瓦片并短 TTL 缓存。
5. 并发控制：
   - 对同一瓦片做 in-flight 去重，减少重复回源请求。
6. 响应输出：
   - 返回瓦片字节流并设置 `Cache-Control`。

## 免责声明

1. 使用者需自行确保对各上游地图服务的调用符合其服务条款、授权与法律法规。
2. 不同服务商可能存在区域覆盖限制、归因要求、缓存限制、API Key 与计费规则。
3. 本项目仅提供技术实现，不构成法律意见或合规承诺。
4. 在生产环境或对外分发场景中使用，风险由使用者自行承担。

## 开源协议

Apache-2.0
