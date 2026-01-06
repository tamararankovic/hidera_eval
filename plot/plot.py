import csv
import os
import sys
import matplotlib.pyplot as plt

# --------------------------------------------------
# Config
# --------------------------------------------------

BASE_DIR = "/home/tamara/experiments"
# BASE_DIR = "/Users/tamararankovic/Documents/monitoring/impl/hidera_eval/tmp"

PROTOCOLS = ["hi", "fu", "ep", "dd", "rr"]
COLORS = {
    "hi": "tab:blue",
    "fu": "tab:orange",
    "ep": "tab:green",
    "dd": "tab:red",
    "rr": "tab:purple",
}

# --------------------------------------------------
# Args
# --------------------------------------------------

if len(sys.argv) < 2:
    print("Usage: python plot_results.py <experiment-name>")
    sys.exit(1)

EXPERIMENT = sys.argv[1]

ANALYZED_DIR = os.path.join(BASE_DIR, f"{EXPERIMENT}_analyzed")
PLOTS_DIR = os.path.join(BASE_DIR, f"{EXPERIMENT}_plots")

os.makedirs(PLOTS_DIR, exist_ok=True)

# --------------------------------------------------
# CSV readers
# --------------------------------------------------

def read_value_csv(path):
    ts, vals = [], []
    with open(path) as f:
        for row in csv.reader(f):
            ts.append(int(row[0]))
            vals.append(float(row[1]))
    return ts, vals


def read_msgcount_csv(path):
    ts, sent, rcvd = [], [], []
    with open(path) as f:
        for row in csv.reader(f):
            ts.append(int(row[0]))
            sent.append(int(row[1]))
            rcvd.append(int(row[2]))
    return ts, sent, rcvd


def read_msgrate_csv(path):
    ts, sent, rcvd = [], [], []
    with open(path) as f:
        for row in csv.reader(f):
            ts.append(int(row[0]))
            sent.append(float(row[1]))
            rcvd.append(float(row[2]))
    return ts, sent, rcvd

# --------------------------------------------------
# Load expected values
# --------------------------------------------------

expected_path = os.path.join(ANALYZED_DIR, "value_expected.csv")
ts_exp, exp_vals = read_value_csv(expected_path)
expected_map = dict(zip(ts_exp, exp_vals))

# --------------------------------------------------
# 1. Expected vs real value
# --------------------------------------------------

plt.figure(figsize=(10, 5))
plt.plot(ts_exp, exp_vals, "--", color="black", label="expected")

for proto in PROTOCOLS:
    path = os.path.join(ANALYZED_DIR, f"{proto}_value_averaged.csv")
    if not os.path.exists(path):
        continue
    ts, vals = read_value_csv(path)
    plt.plot(ts, vals, color=COLORS[proto], label=proto)

plt.xlabel("time [s]")
plt.ylabel("value")
plt.title("Expected vs Real Value")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "value_expected_vs_real.png"))
plt.close()

# --------------------------------------------------
# 2. Message count
# --------------------------------------------------

plt.figure(figsize=(10, 5))

for proto in PROTOCOLS:
    path = os.path.join(ANALYZED_DIR, f"{proto}_msgcount_averaged.csv")
    if not os.path.exists(path):
        continue
    ts, sent, rcvd = read_msgcount_csv(path)
    plt.plot(ts, sent, color=COLORS[proto], linestyle="-", label=f"{proto} sent")
    plt.plot(ts, rcvd, color=COLORS[proto], linestyle="--", label=f"{proto} rcvd")

plt.xlabel("time [s]")
plt.ylabel("message count")
plt.title("Message Count (Sent / Received)")
plt.legend(ncol=2)
plt.grid(True)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "msg_count.png"))
plt.close()

# --------------------------------------------------
# 3. Message rate
# --------------------------------------------------

plt.figure(figsize=(10, 5))

for proto in PROTOCOLS:
    path = os.path.join(ANALYZED_DIR, f"{proto}_msgrate_averaged.csv")
    if not os.path.exists(path):
        continue
    ts, sent, rcvd = read_msgrate_csv(path)
    plt.plot(ts, sent, color=COLORS[proto], linestyle="-", label=f"{proto} sent")
    plt.plot(ts, rcvd, color=COLORS[proto], linestyle="--", label=f"{proto} rcvd")

plt.xlabel("time [s]")
plt.ylabel("msg/s")
plt.title("Message Rate")
plt.legend(ncol=2)
plt.grid(True)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "msg_rate.png"))
plt.close()

# --------------------------------------------------
# 4. Mean Absolute Error
# --------------------------------------------------

plt.figure(figsize=(10, 5))

for proto in PROTOCOLS:
    path = os.path.join(ANALYZED_DIR, f"{proto}_value_averaged.csv")
    if not os.path.exists(path):
        continue

    ts, vals = read_value_csv(path)
    mae_ts, mae_vals = [], []

    for t, v in zip(ts, vals):
        if t in expected_map:
            mae_ts.append(t)
            mae_vals.append(abs(v - expected_map[t]))

    plt.plot(mae_ts, mae_vals, color=COLORS[proto], label=proto)

plt.xlabel("time [s]")
plt.ylabel("MAE")
plt.title("Mean Absolute Error")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "mae.png"))
plt.close()

# --------------------------------------------------
# 5. Scatter plot: real vs expected
# --------------------------------------------------

plt.figure(figsize=(12, 6))

# Plot expected value
plt.plot(
    ts_exp,
    exp_vals,
    color="black",
    linestyle="--",
    linewidth=2,
    label="expected",
)

for proto in PROTOCOLS:
    color = COLORS[proto]

    for fname in os.listdir(ANALYZED_DIR):
        # Match per-node files: <proto>_value_<node>.csv
        if not fname.startswith(f"{proto}_value_"):
            continue
        if fname.endswith("_averaged.csv"):
            continue

        path = os.path.join(ANALYZED_DIR, fname)
        ts, vals = read_value_csv(path)

        # Plot each node as a thin, semi-transparent line
        plt.plot(
            ts,
            vals,
            color=color,
            alpha=0.25,
            linewidth=1,
            label=proto,
        )

# Deduplicate legend entries (one per protocol + expected)
handles, labels = plt.gca().get_legend_handles_labels()
unique = dict(zip(labels, handles))
plt.legend(unique.values(), unique.keys(), ncol=2)

plt.xlabel("time [s]")
plt.ylabel("value")
plt.title("Node Values vs Expected Value (All Nodes)")
plt.grid(True)
plt.tight_layout()
plt.savefig(os.path.join(PLOTS_DIR, "value_scatter.png"))
plt.close()



print(f"Plots saved to: {PLOTS_DIR}")
