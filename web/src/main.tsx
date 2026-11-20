import "@mantine/core/styles/baseline.css";
import "@mantine/core/styles/default-css-variables.css";
import "@mantine/core/styles/global.css";
import "@mantine/core/styles/ActionIcon.css";
import "@mantine/core/styles/Alert.css";
import "@mantine/core/styles/Badge.css";
import "@mantine/core/styles/Button.css";
import "@mantine/core/styles/CloseButton.css";
import "@mantine/core/styles/Combobox.css";
import "@mantine/core/styles/Group.css";
import "@mantine/core/styles/InlineInput.css";
import "@mantine/core/styles/Input.css";
import "@mantine/core/styles/Loader.css";
import "@mantine/core/styles/Menu.css";
import "@mantine/core/styles/Modal.css";
import "@mantine/core/styles/ModalBase.css";
import "@mantine/core/styles/Notification.css";
import "@mantine/core/styles/NumberInput.css";
import "@mantine/core/styles/Overlay.css";
import "@mantine/core/styles/Pagination.css";
import "@mantine/core/styles/Paper.css";
import "@mantine/core/styles/PasswordInput.css";
import "@mantine/core/styles/Popover.css";
import "@mantine/core/styles/ScrollArea.css";
import "@mantine/core/styles/Stepper.css";
import "@mantine/core/styles/Switch.css";
import "@mantine/core/styles/Table.css";
import "@mantine/core/styles/Tabs.css";
import "@mantine/core/styles/UnstyledButton.css";
import "@mantine/core/styles/VisuallyHidden.css";
import { createTheme, MantineProvider } from "@mantine/core";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import { I18nProvider } from "./i18n";
import "./styles.css";

const theme = createTheme({
  primaryColor: "indigo",
  defaultRadius: "sm",
  fontFamily:
    "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif",
});

const root = document.getElementById("root");
if (!root) throw new Error("Missing application root element");

createRoot(root).render(
  <StrictMode>
    <I18nProvider>
      <MantineProvider theme={theme} defaultColorScheme="dark">
        <App />
      </MantineProvider>
    </I18nProvider>
  </StrictMode>,
);
