import { Button, Menu, Modal, Notification } from "@mantine/core";
import {
  AlertTriangle,
  Check,
  Languages,
  LayoutDashboard,
  LoaderCircle,
  Plus,
  Server,
  Trash2,
  Waves,
  X,
} from "lucide-react";
import { lazy, Suspense, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import * as api from "./api";
import { type Locale, localeStorageKey } from "./i18n";
import { Overview } from "./Overview";
import { type NodeFilter, Upstreams } from "./Upstreams";

const NodeDetails = lazy(() =>
  import("./NodeDetails").then((module) => ({ default: module.NodeDetails })),
);

import type { EditorState } from "./NodeEditor";

const NodeEditor = lazy(() =>
  import("./NodeEditor").then((module) => ({ default: module.NodeEditor })),
);

import { createDraft, draftToConfig, recordToDraft, shouldClearApiKey } from "./node-config";
import type { NodeRecord } from "./types";
import { useControlPlane } from "./use-control-plane";

type View = "overview" | "upstreams";
interface ToastState {
  tone: "success" | "error";
  message: string;
}

function LanguageSwitch() {
  const { t, i18n } = useTranslation();
  const locale: Locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
  const select = (next: Locale) => {
    window.localStorage.setItem(localeStorageKey, next);
    void i18n.changeLanguage(next);
  };
  return (
    <Menu position="bottom-end" shadow="md" width={160} withinPortal>
      <Menu.Target>
        <button
          type="button"
          className="language-trigger"
          aria-label={t("language.label")}
          title={t("language.label")}
        >
          <Languages size={15} />
          <span>{locale === "zh-CN" ? "中文" : "EN"}</span>
        </button>
      </Menu.Target>
      <Menu.Dropdown>
        <Menu.Item
          rightSection={locale === "en" ? <Check size={13} /> : null}
          onClick={() => select("en")}
        >
          {t("language.english")}
        </Menu.Item>
        <Menu.Item
          rightSection={locale === "zh-CN" ? <Check size={13} /> : null}
          onClick={() => select("zh-CN")}
        >
          {t("language.chinese")}
        </Menu.Item>
      </Menu.Dropdown>
    </Menu>
  );
}

export default function App() {
  const { t, i18n } = useTranslation();
  const [view, setView] = useState<View>("overview");
  const {
    nodes,
    status,
    loading,
    refreshing,
    connectionError,
    lastSync,
    refresh,
    nodesStale,
    statusStale,
    nodesLoaded,
  } = useControlPlane();
  const [editor, setEditor] = useState<EditorState | null>(null);
  const [editorDirty, setEditorDirty] = useState(false);
  const [conflict, setConflict] = useState<NodeRecord | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState<NodeRecord | null>(null);
  const [toast, setToast] = useState<ToastState | null>(null);
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<NodeFilter>("all");

  useEffect(() => {
    const locale = i18n.resolvedLanguage === "zh-CN" ? "zh-CN" : "en";
    document.documentElement.lang = locale;
    document.title = t("app.title");
  }, [i18n.resolvedLanguage, t]);

  useEffect(() => {
    if (!toast) return;
    const timeout = window.setTimeout(() => setToast(null), 6000);
    return () => window.clearTimeout(timeout);
  }, [toast]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: Reset scroll when the selected page changes.
  useEffect(() => {
    window.scrollTo(0, 0);
  }, [view, selectedNodeId, editor?.mode]);

  const selectedNode = nodes.find((node) => node.config.id === selectedNodeId) ?? null;
  const openAdd = () => {
    setConflict(null);
    setEditorDirty(false);
    setSelectedNodeId(null);
    setEditor({ mode: "create", draft: createDraft(), revision: null });
  };
  const openEdit = (node: NodeRecord) => {
    setConflict(null);
    setEditorDirty(false);
    setSelectedNodeId(null);
    setEditor({ mode: "edit", draft: recordToDraft(node), revision: node.revision });
  };
  const changeView = (next: View) => {
    if (busy || (editorDirty && !window.confirm(t("editor.discard")))) return;
    setEditor(null);
    setEditorDirty(false);
    setConflict(null);
    setSelectedNodeId(null);
    setView(next);
  };

  const save = async (state: EditorState) => {
    setBusy(true);
    try {
      const config = draftToConfig(state.draft);
      if (state.mode === "create") await api.createNode(config);
      else await api.updateNode(config, state.revision, shouldClearApiKey(state.draft));
      setEditor(null);
      setEditorDirty(false);
      setConflict(null);
      setView("upstreams");
      setToast({
        tone: "success",
        message: state.mode === "create" ? t("toast.nodeAdded") : t("toast.nodeUpdated"),
      });
      await refresh(true);
    } catch (error) {
      setToast({
        tone: "error",
        message: error instanceof Error ? error.message : t("toast.operationFailed"),
      });
      if (error instanceof api.ApiError && error.code === "revision_conflict") {
        try {
          setConflict(await api.getNode(state.draft.id));
        } catch (readError) {
          setToast({
            tone: "error",
            message: readError instanceof Error ? readError.message : t("toast.operationFailed"),
          });
        }
        await refresh(true);
      }
    } finally {
      setBusy(false);
    }
  };

  const toggleDrain = async (node: NodeRecord) => {
    setBusy(true);
    try {
      await api.setDraining(node.config.id, node.runtime.lifecycle === "serving");
      setToast({
        tone: "success",
        message:
          node.runtime.lifecycle === "serving" ? t("toast.nodeDraining") : t("toast.nodeResumed"),
      });
      await refresh(true);
    } catch (error) {
      setToast({
        tone: "error",
        message: error instanceof Error ? error.message : t("toast.lifecycleFailed"),
      });
    } finally {
      setBusy(false);
    }
  };

  const remove = async () => {
    if (!confirmDelete) return;
    setBusy(true);
    try {
      await api.deleteNode(confirmDelete.config.id, confirmDelete.revision);
      setToast({ tone: "success", message: t("toast.nodeDeleted") });
      setConfirmDelete(null);
      setSelectedNodeId(null);
      setView("upstreams");
      await refresh(true);
    } catch (error) {
      setToast({
        tone: "error",
        message: error instanceof Error ? error.message : t("toast.deleteFailed"),
      });
    } finally {
      setBusy(false);
    }
  };

  const setIpLimit = async (ip: string, limit: number) => {
    try {
      await api.setIpLimit(ip, limit);
      setToast({ tone: "success", message: t("toast.ipLimitSaved") });
      await refresh(true);
      return true;
    } catch (error) {
      setToast({
        tone: "error",
        message: error instanceof Error ? error.message : t("toast.ipLimitFailed"),
      });
      return false;
    }
  };

  const deleteIpLimit = async (ip: string) => {
    try {
      await api.deleteIpLimit(ip);
      await refresh(true);
    } catch (error) {
      setToast({
        tone: "error",
        message: error instanceof Error ? error.message : t("toast.ipLimitFailed"),
      });
    }
  };

  return (
    <div className="app-shell">
      <aside className="desktop-sidebar">
        <div className="brand">
          <Waves size={25} />
          <strong>Estuary</strong>
        </div>
        <nav aria-label={t("nav.label")}>
          <button
            type="button"
            className={view === "overview" && !selectedNode && !editor ? "active" : ""}
            onClick={() => changeView("overview")}
          >
            <LayoutDashboard size={16} />
            {t("nav.overview")}
          </button>
          <button
            type="button"
            className={view === "upstreams" || selectedNode || editor ? "active" : ""}
            onClick={() => changeView("upstreams")}
          >
            <Server size={16} />
            {t("nav.upstreams")}
          </button>
        </nav>
        <div className="sidebar-footer">
          <LanguageSwitch />
          <div className="system-status">
            <span>{t("controlPlane.label")}</span>
            <div>
              <i className={connectionError ? "down" : ""} />
              <strong>
                {loading
                  ? t("common.loading")
                  : connectionError
                    ? t("controlPlane.disconnected")
                    : t("controlPlane.connected")}
              </strong>
              <small>estuary-admin</small>
            </div>
          </div>
        </div>
      </aside>

      <div className="mobile-toolbar">
        <div className="brand">
          <Waves size={21} />
          <strong>Estuary</strong>
        </div>
        <LanguageSwitch />
      </div>

      <main className="main-content">
        {connectionError && (
          <div className="connection-banner" role="alert">
            <AlertTriangle size={16} />
            <span>
              <strong>{t("controlPlane.unavailable")}</strong>
              {t("controlPlane.stale", { error: connectionError })}
            </span>
            <Button variant="default" size="compact-sm" onClick={() => void refresh()}>
              {t("common.retry")}
            </Button>
          </div>
        )}
        {(nodesStale || statusStale) && (
          <div className="data-status" role="status">
            {t("data.stale")} · {t("data.partial")}
          </div>
        )}
        <Suspense
          fallback={
            <div className="empty-state" role="status">
              <LoaderCircle className="spin" size={20} />
              {t("common.loading")}
            </div>
          }
        >
          {editor ? (
            <NodeEditor
              key={`${editor.mode}:${editor.draft.id}`}
              state={editor}
              busy={busy}
              conflict={conflict}
              onResolveConflict={() => setConflict(null)}
              onDirtyChange={setEditorDirty}
              onClose={() => {
                setEditor(null);
                setEditorDirty(false);
                setConflict(null);
              }}
              onSave={save}
            />
          ) : selectedNode ? (
            <NodeDetails
              node={selectedNode}
              busy={busy}
              onClose={() => setSelectedNodeId(null)}
              onEdit={() => openEdit(selectedNode)}
              onToggleDrain={() => void toggleDrain(selectedNode)}
              onDelete={() => setConfirmDelete(selectedNode)}
            />
          ) : view === "overview" ? (
            <Overview
              loading={loading}
              nodesLoaded={nodesLoaded}
              status={status}
              nodes={nodes}
              lastSync={lastSync}
              refreshing={refreshing}
              onRefresh={() => void refresh()}
              onAdd={openAdd}
              onShowNodes={() => changeView("upstreams")}
              onSelectNode={(node) => setSelectedNodeId(node.config.id)}
              onSetIpLimit={setIpLimit}
              onDeleteIpLimit={deleteIpLimit}
            />
          ) : (
            <Upstreams
              nodes={nodes}
              busy={busy}
              loading={loading}
              query={query}
              filter={filter}
              lastSync={lastSync}
              refreshing={refreshing}
              onQuery={setQuery}
              onFilter={setFilter}
              onRefresh={() => void refresh()}
              onSelect={(node) => setSelectedNodeId(node.config.id)}
              onEdit={openEdit}
              onToggleDrain={(node) => void toggleDrain(node)}
              onDelete={setConfirmDelete}
              onAdd={openAdd}
            />
          )}
        </Suspense>
      </main>

      {!editor && !selectedNode && (
        <nav className="mobile-bottom-nav" aria-label={t("nav.label")}>
          <button
            type="button"
            className={view === "overview" ? "active" : ""}
            onClick={() => changeView("overview")}
          >
            <LayoutDashboard size={16} />
            {t("nav.overview")}
          </button>
          <button
            type="button"
            className={view === "upstreams" ? "active" : ""}
            onClick={() => changeView("upstreams")}
          >
            <Server size={16} />
            {t("nav.upstreams")}
          </button>
          <button type="button" onClick={openAdd}>
            <Plus size={16} />
            {t("nav.add")}
          </button>
        </nav>
      )}

      <Modal
        opened={Boolean(confirmDelete)}
        onClose={() => !busy && setConfirmDelete(null)}
        title={t("delete.title", { id: confirmDelete?.config.id ?? "node" })}
        centered
      >
        <div className="delete-dialog">
          <div className="delete-icon">
            <Trash2 size={18} />
          </div>
          <p>{t("delete.description")}</p>
          <div>
            <Button variant="default" disabled={busy} onClick={() => setConfirmDelete(null)}>
              {t("common.cancel")}
            </Button>
            <Button
              color="red"
              disabled={busy}
              leftSection={
                busy ? <LoaderCircle className="spin" size={14} /> : <Trash2 size={14} />
              }
              onClick={() => void remove()}
            >
              {t("delete.node")}
            </Button>
          </div>
        </div>
      </Modal>

      {toast && (
        <Notification
          className="app-notification"
          color={toast.tone === "success" ? "green" : "red"}
          icon={toast.tone === "success" ? <Check size={15} /> : <X size={15} />}
          withCloseButton
          onClose={() => setToast(null)}
        >
          {toast.message}
        </Notification>
      )}
    </div>
  );
}
