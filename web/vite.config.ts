import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const adminProxy = env.VITE_ADMIN_PROXY || "http://127.0.0.1:9090";
  return {
    base: "/admin/",
    plugins: [react()],
    server: {
      proxy: {
        "/admin/api": adminProxy,
        "/health": adminProxy,
      },
    },
  };
});
