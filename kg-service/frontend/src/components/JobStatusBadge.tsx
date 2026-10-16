import type { JobStatus } from "../types";

export default function JobStatusBadge({ status }: { status: JobStatus }) {
  return <span className={`badge ${status}`}>{status}</span>;
}
