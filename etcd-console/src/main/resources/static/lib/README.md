# Console UI Libs

当前运行环境无法访问 CDN 下载二进制前端库文件，因此本包默认在 `index.html` 中使用 cdnjs 引入：

- Vue 2.7.16
- Axios 1.6.8
- Element UI 2.15.14

如需改成完全离线本地引入，请在可联网环境执行项目根目录下的：

```bash
bash scripts/download-console-ui-libs.sh
```

脚本会下载到：

```text
etcd-console/src/main/resources/static/lib/vue/vue.min.js
etcd-console/src/main/resources/static/lib/axios/axios.min.js
etcd-console/src/main/resources/static/lib/element-ui/index.min.js
etcd-console/src/main/resources/static/lib/element-ui/theme-chalk/index.min.css
etcd-console/src/main/resources/static/lib/element-ui/theme-chalk/fonts/element-icons.woff
etcd-console/src/main/resources/static/lib/element-ui/theme-chalk/fonts/element-icons.ttf
```
