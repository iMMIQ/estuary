import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { gzipSync } from "node:zlib";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const adminProxy = env.VITE_ADMIN_PROXY || "http://127.0.0.1:9090";
  return {
    base: "/admin/",
    plugins: [react(), {
      name: "precompress-admin-assets",
      apply: "build",
      generateBundle: {
        // Vite finalizes preload references and CSS in its own bundle hooks.
        order: "post",
        handler(_, bundle) {
          for (const asset of Object.values(bundle)) {
            if (!/\.(js|css)$/.test(asset.fileName)) continue;
            const source = asset.type === "chunk" ? asset.code : asset.source;
            this.emitFile({ type: "asset", fileName: `${asset.fileName}.gz`,
              source: gzipSync(source, { level: 9 }) });
          }
        },
      },
    }],
    server: {
      proxy: {
        "/admin/api": adminProxy,
        "/health": adminProxy,
      },
    },
  };
});
