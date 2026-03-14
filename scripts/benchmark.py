import subprocess
import time
import matplotlib.pyplot as plt
import os
import sys

# Add the parent directory to sys.path so we can import config if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# Ensure the docs directory exists for saving the plots
output_dir = "docs"
os.makedirs(output_dir, exist_ok=True)

# Define configurations keeping within the course quota: Max 4 VMs, ssc.medium (2 vCPU, 4GB RAM)
# We define them here instead of config.py so we can tightly control cores and memory per run.
configurations = [
    # Vertical scaling tests (1 worker: changing cores and memory)
    {"name": "1_worker_1core_2g", "executors": "1", "cores": "1", "memory": "2g", "type": "vertical"},
    
    # Horizontal scaling tests (1 to 3 workers, maximizing the 2vCPU/4GB node limits)
    # The 1_worker_2core_4g acts as both the top-end vertical test AND the baseline for horizontal
    {"name": "1_worker_2core_4g", "executors": "1", "cores": "2", "memory": "4g", "type": "horizontal"}, 
    {"name": "2_workers",         "executors": "2", "cores": "2", "memory": "4g", "type": "horizontal"},
    {"name": "3_workers",         "executors": "3", "cores": "2", "memory": "4g", "type": "horizontal"}
]

# Paths relative to the root of the project (assuming script is run from root)
script_path = "src/etl_job.py"
master_url = config.SPARK_MASTER

results = {}

print("Starting Spark benchmark tests...")
print(f"Master URL: {master_url}")
print(f"Target Script: {script_path}\n")

for conf in configurations:
    print(f"--------------------------------------------------")
    print(f"[Benchmarking] Running config: {conf['name']}")
    print(f"Executors: {conf['executors']}, Cores/Exec: {conf['cores']}, RAM/Exec: {conf['memory']}")
    
    # Construct the spark-submit command
    cmd = [
        "spark-submit",
        "--master", master_url,
        "--num-executors", conf["executors"],
        "--executor-cores", conf["cores"],
        "--executor-memory", conf["memory"],
        script_path
    ]
    
    start_time = time.time()
    
    # Run the job. capture_output=True hides standard Spark logs to keep console clean
    process = subprocess.run(cmd, capture_output=True, text=True) 
    
    end_time = time.time()
    
    if process.returncode == 0:
        duration = end_time - start_time
        results[conf["name"]] = duration
        print(f"Success! Runtime: {duration:.2f} seconds")
    else:
        print(f"Job failed for {conf['name']}. Here is the end of the error log:")
        # Print the last 1000 characters to help debug
        print(process.stderr[-1000:]) 
        results[conf["name"]] = None

print("\n==================================================")
print("Benchmarking complete. Generating plots...")

# --- Plotting Logic ---

# 1. Filter successfully completed horizontal runs
h_configs = [c for c in configurations if c["type"] == "horizontal" and results[c["name"]] is not None]

if len(h_configs) > 1:
    workers = [int(c["executors"]) for c in h_configs]
    runtimes = [results[c["name"]] for c in h_configs]
    
    # Calculate Speedup (Time_1 / Time_N)
    t1 = runtimes[0] 
    actual_speedup = [t1 / t for t in runtimes]
    ideal_speedup = workers # Ideal linear speedup matches the number of workers

    plt.figure(figsize=(12, 5))

    # Plot 1: Runtime vs Workers
    plt.subplot(1, 2, 1)
    plt.plot(workers, runtimes, marker='o', linestyle='-', color='b', linewidth=2)
    plt.title('Runtime vs. Number of Workers')
    plt.xlabel('Number of Workers')
    plt.ylabel('Runtime (seconds)')
    plt.xticks(workers)
    plt.grid(True, linestyle='--', alpha=0.7)

    # Plot 2: Speedup vs Ideal Linear Speedup
    plt.subplot(1, 2, 2)
    plt.plot(workers, actual_speedup, marker='o', linestyle='-', color='g', linewidth=2, label='Actual Speedup')
    plt.plot(workers, ideal_speedup, marker='x', linestyle='--', color='r', linewidth=2, label='Ideal Linear Speedup')
    plt.title('Speedup Factor vs. Ideal')
    plt.xlabel('Number of Workers')
    plt.ylabel('Speedup Factor ($Time_1 / Time_N$)')
    plt.xticks(workers)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)

    plt.tight_layout()
    
    # Save the figure to the docs folder
    plot_path = os.path.join(output_dir, "horizontal_scaling_results.png")
    plt.savefig(plot_path)
    print(f"Horizontal scaling plots successfully saved to {plot_path}")
else:
    print("Not enough successful horizontal runs to generate plots.")

# 2. Check and print vertical scaling results
v_configs = [c for c in configurations if c["name"] in ["1_worker_1core_2g", "1_worker_2core_4g"] and results[c["name"]] is not None]

if len(v_configs) == 2:
    print("\n--- Vertical Scaling Results ---")
    t_low = results["1_worker_1core_2g"]
    t_high = results["1_worker_2core_4g"]
    print(f"1 Core / 2GB RAM Runtime:  {t_low:.2f} seconds")
    print(f"2 Cores / 4GB RAM Runtime: {t_high:.2f} seconds")
    print(f"Speedup Factor: {t_low / t_high:.2f}x")