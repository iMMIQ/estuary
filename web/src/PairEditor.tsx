import { Button, Select, Switch, TextInput } from "@mantine/core";
import { Plus, Trash2 } from "lucide-react";
import { useRef } from "react";
import { useTranslation } from "react-i18next";
import { configDefaults as defaults } from "./config-contract";
import type { TranslationKey } from "./i18n";
import type { ModelCapabilityConfig, Pair } from "./types";

export function PairEditor({
  rows,
  error,
  onChange,
  keyLabel,
  valueLabel,
  inherited,
}: {
  inherited?: ModelCapabilityConfig;
  rows: Pair[];
  error?: string;
  onChange: (rows: Pair[]) => void;
  keyLabel: string;
  valueLabel: string;
}) {
  const { t } = useTranslation();
  const rowKeys = useRef(new WeakMap<Pair, string>());
  const nextRowKey = useRef(0);
  const rowKey = (row: Pair) => {
    let key = rowKeys.current.get(row);
    if (!key) {
      key = String(nextRowKey.current++);
      rowKeys.current.set(row, key);
    }
    return key;
  };
  const updateRow = (row: Pair, change: Partial<Pair>): Pair => {
    const updated = { ...row, ...change };
    rowKeys.current.set(updated, rowKey(row));
    return updated;
  };
  const update = (index: number, key: keyof Pair, value: string | boolean) => {
    onChange(
      rows.map((row, rowIndex) =>
        rowIndex === index
          ? updateRow(row, {
              ...(row.inherit_capability && ["family", "multimodal"].includes(key)
                ? {
                    family: inherited?.family ?? defaults.capability.family,
                    multimodal: inherited?.multimodal ?? defaults.capability.multimodal,
                  }
                : {}),
              [key]: value,
              ...(["family", "multimodal"].includes(key) || (key === "key" && value === "*")
                ? { inherit_capability: false }
                : {}),
            })
          : row,
      ),
    );
  };

  return (
    <div className="mapping-editor">
      <div className="mapping-head">
        <span>{keyLabel}</span>
        <span>{valueLabel}</span>
        <span>{t("editor.multimodal")}</span>
        <span>{t("editor.modelFamily")}</span>
        <span />
      </div>
      {rows.map((row, index) => (
        <div className="mapping-row" key={rowKey(row)}>
          <TextInput
            aria-label={`${keyLabel} ${index + 1}`}
            value={row.key}
            error={Boolean(error)}
            onChange={(event) => update(index, "key", event.target.value)}
          />
          <TextInput
            aria-label={`${valueLabel} ${index + 1}`}
            value={row.value}
            error={Boolean(error)}
            onChange={(event) => update(index, "value", event.target.value)}
          />
          <Switch
            size="sm"
            aria-label={`${t("editor.multimodal")} ${index + 1}`}
            checked={
              row.inherit_capability
                ? (inherited?.multimodal ?? defaults.capability.multimodal)
                : (row.multimodal ?? defaults.capability.multimodal)
            }
            onChange={(event) => update(index, "multimodal", event.currentTarget.checked)}
          />
          <Select
            aria-label={`${t("editor.modelFamily")} ${index + 1}`}
            value={row.inherit_capability ? "inherit" : (row.family ?? defaults.capability.family)}
            data={[
              {
                value: "inherit",
                label: t("editor.inheritedFamily", {
                  family: t(`family.${inherited?.family ?? defaults.capability.family}`),
                }),
              },
              { value: "generic", label: t("family.generic") },
              { value: "deepseek", label: t("family.deepseek") },
            ]}
            onChange={(value) => {
              if (value === "inherit")
                onChange(
                  rows.map((item, i) =>
                    i === index
                      ? updateRow(item, {
                          inherit_capability: true,
                          family: inherited?.family ?? defaults.capability.family,
                          multimodal: inherited?.multimodal ?? defaults.capability.multimodal,
                        })
                      : item,
                  ),
                );
              else if (value) update(index, "family", value);
            }}
          />
          <Button
            variant="subtle"
            color="gray"
            px={6}
            aria-label={t("editor.removeMapping", { count: index + 1 })}
            onClick={() =>
              onChange(
                rows.length === 1
                  ? [{ key: "", value: "" }]
                  : rows.filter((_, rowIndex) => rowIndex !== index),
              )
            }
          >
            <Trash2 size={14} />
          </Button>
        </div>
      ))}
      {error && (
        <span className="form-error" role="alert">
          {t(error as TranslationKey)}
        </span>
      )}
      <Button
        variant="subtle"
        size="compact-sm"
        leftSection={<Plus size={14} />}
        onClick={() => onChange([...rows, { key: "", value: "", ...defaults.capability }])}
      >
        {t("editor.addMapping")}
      </Button>
    </div>
  );
}
