import { useEffect } from "react";

export type Confirmable = {
  title: string;
  message: string;
  confirmLabel: string;
  tone?: "danger" | "primary";
  run: () => void;
};

type Props = {
  pending: Confirmable | null;
  onClose: () => void;
};

export default function ConfirmDialog({ pending, onClose }: Props) {
  useEffect(() => {
    if (!pending) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [pending, onClose]);

  if (!pending) return null;
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div
        className="modal-card"
        role="dialog"
        aria-modal="true"
        onClick={(e) => e.stopPropagation()}
      >
        <h3 className="modal-title">{pending.title}</h3>
        <p className="modal-msg">{pending.message}</p>
        <div className="row" style={{ justifyContent: "flex-end", marginTop: 18 }}>
          <button className="secondary" onClick={onClose}>
            Cancel
          </button>
          <button
            className={pending.tone === "danger" ? "danger" : "primary"}
            onClick={() => {
              pending.run();
              onClose();
            }}
          >
            {pending.confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
