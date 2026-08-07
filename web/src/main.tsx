import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { createTheme, MantineProvider } from "@mantine/core";
import "@mantine/core/styles.css";
import "./i18n";
import App from "./App";
import "./styles.css";

const theme = createTheme({
  primaryColor: "indigo",
  defaultRadius: "sm",
  fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif",
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <MantineProvider theme={theme} defaultColorScheme="dark">
      <App />
    </MantineProvider>
  </StrictMode>,
);
