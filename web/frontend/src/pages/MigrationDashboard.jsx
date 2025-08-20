import React, { useEffect, useState } from "react"

const fmtUSD = (v) => (v == null ? "-" : `$${Number(v).toFixed(2)}`)
const fmtTime = (t) => (t ? new Date(t * 1000).toLocaleString() : "-")

export default function MigrationDashboard() {
  const [runs, setRuns] = useState([])
  const [selected, setSelected] = useState(null)
  const [detail, setDetail] = useState(null)

  useEffect(() => {
    fetch("/api/migration/runs").then(r => r.json()).then(d => setRuns(d.runs || []))
  }, [])

  const openDetail = async (id) => {
    setSelected(id)
    const r = await fetch(`/api/migration/runs/${id}`)
    const d = await r.json()
    setDetail(d)
  }

  return (
    <div className="max-w-5xl mx-auto p-6 space-y-6">
      <h1 className="text-2xl font-bold">Migration Reports</h1>

      <div className="bg-white rounded-xl shadow-sm border p-4">
        <table className="w-full text-sm">
          <thead className="text-left text-gray-600">
            <tr>
              <th className="py-2">Report</th>
              <th className="py-2">Started</th>
              <th className="py-2">Duration</th>
              <th className="py-2">Events Sent</th>
              <th className="py-2">MTU</th>
              <th className="py-2">Est. Cost</th>
              <th className="py-2">Mode</th>
              <th className="py-2"></th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.id} className="border-t">
                <td className="py-2 font-mono">{r.id}</td>
                <td className="py-2">{fmtTime(r.started_at)}</td>
                <td className="py-2">{r.duration_s}s</td>
                <td className="py-2">{r.events_sent}</td>
                <td className="py-2">{r.mtu_estimate}</td>
                <td className="py-2">{fmtUSD(r.estimated_cost_usd)}</td>
                <td className="py-2">{r.dry_run ? "Dry run" : "Real"}</td>
                <td className="py-2">
                  <button
                    onClick={() => openDetail(r.id)}
                    className="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700"
                  >
                    View
                  </button>
                </td>
              </tr>
            ))}
            {runs.length === 0 && (
              <tr><td colSpan={8} className="py-6 text-center text-gray-500">No reports yet.</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {detail && (
        <div className="bg-white rounded-xl shadow-sm border p-4">
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-lg font-semibold">Report: {selected}</h2>
            <button onClick={() => setDetail(null)} className="text-sm text-blue-600 hover:underline">Close</button>
          </div>
          <pre className="text-xs bg-gray-50 rounded-lg p-3 overflow-auto max-h-[400px]">
            {JSON.stringify(detail, null, 2)}
          </pre>
        </div>
      )}
    </div>
  )
}