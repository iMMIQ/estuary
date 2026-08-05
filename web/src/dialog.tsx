import { X } from "lucide-react";
import { type ReactNode, useEffect, useRef } from "react";

const focusableSelector = [
  "button:not([disabled])",
  "input:not([disabled])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  "a[href]",
  "[tabindex]:not([tabindex='-1'])",
].join(",");

export function Drawer({
  title,
  eyebrow,
  ariaLabel,
  busy = false,
  onClose,
  children,
}: {
  title: string;
  eyebrow: string;
  ariaLabel: string;
  busy?: boolean;
  onClose: () => void;
  children: ReactNode;
}) {
  const drawerRef = useRef<HTMLElement>(null);
  const onCloseRef = useRef(onClose);
  const busyRef = useRef(busy);

  useEffect(() => {
    onCloseRef.current = onClose;
    busyRef.current = busy;
  }, [busy, onClose]);

  useEffect(() => {
    const previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const drawer = drawerRef.current;
    const first = drawer?.querySelector<HTMLElement>("[data-autofocus]")
      ?? drawer?.querySelector<HTMLElement>(focusableSelector);
    first?.focus();

    const keydown = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !busyRef.current) {
        event.preventDefault();
        onCloseRef.current();
        return;
      }
      if (event.key !== "Tab" || !drawer) return;
      const focusable = [...drawer.querySelectorAll<HTMLElement>(focusableSelector)]
        .filter((element) => element.offsetParent !== null);
      if (focusable.length === 0) return;
      const firstElement = focusable[0];
      const lastElement = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === firstElement) {
        event.preventDefault();
        lastElement.focus();
      } else if (!event.shiftKey && document.activeElement === lastElement) {
        event.preventDefault();
        firstElement.focus();
      }
    };

    document.addEventListener("keydown", keydown);
    return () => {
      document.removeEventListener("keydown", keydown);
      previousFocus?.focus();
    };
  }, []);

  return (
    <div className="drawer-layer">
      <div className="drawer-scrim" aria-hidden="true" onMouseDown={() => !busy && onClose()} />
      <aside ref={drawerRef} className="drawer" role="dialog" aria-modal="true" aria-label={ariaLabel}>
        <header className="drawer-header">
          <div>
            <span className="eyebrow">{eyebrow}</span>
            <h2>{title}</h2>
          </div>
          <button className="icon-button" type="button" title="Close" aria-label="Close" disabled={busy} onClick={onClose}>
            <X size={18} />
          </button>
        </header>
        {children}
      </aside>
    </div>
  );
}
