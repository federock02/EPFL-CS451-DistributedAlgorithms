import os
import subprocess
import random
import time
import signal

# Generate configuration files for each process
def generate_config_files(num_processes, num_proposals, max_values, distinct_values, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_values = list(range(1, distinct_values + 1))

    for process_id in range(1, num_processes + 1):
        config_path = os.path.join(output_dir, f"lattice-agreement-{process_id}.config")
        with open(config_path, "w") as f:
            f.write(f"{num_proposals} {max_values} {distinct_values}\n")
            for _ in range(num_proposals):
                proposal_set = random.sample(all_values, random.randint(1, max_values))
                f.write(" ".join(map(str, proposal_set)) + "\n")
    print(f"Configuration files written to {output_dir}")

# Run the processes
def run_processes(num_processes, hosts_file, output_dir, config_dir, timeout):
    processes = []
    print("Starting processes...")
    for process_id in range(1, num_processes + 1):
        output_path = os.path.join(output_dir,  "proc{:02d}.out".format(process_id))
        stdout_path = os.path.join(output_dir, "proc{:02d}.stdout".format(process_id))
        stderr_path = os.path.join(output_dir, "proc{:02d}.stderr".format(process_id))
        config_path = os.path.join(config_dir, f"lattice-agreement-{process_id}.config")

        cmd = [
            "java", "-jar", "./bin/da_proc.jar",
            "--id", str(process_id),
            "--hosts", hosts_file,
            "--output", output_path,
            config_path
        ]

        processes.append(subprocess.Popen(cmd, stdout=open(stdout_path,"w"), stderr=open(stderr_path,"w")))
        print(f"Process {process_id} started.")
    print()
    # Wait for a predetermined number of seconds
    time.sleep(timeout)

    # Terminate all processes
    print("Terminating processes...")
    for p in processes:
        print(f"Terminating process {p.pid}...", end="")
        os.kill(p.pid, signal.SIGTERM)
        p.wait()
        print(f"OK")

    print("All processes have completed execution.\n\n")

# Validate output files
def validate_outputs(num_processes, output_dir, config_dir):
    validity_passed = True
    consistency_passed = True
    termination_passed = True

    # Print stderr files
    for process_id in range(1, num_processes + 1):
        stderr_path = os.path.join(output_dir, "proc{:02d}.stderr".format(process_id))
        with open(stderr_path, "r") as f:
            stderr_content = f.read()
            if stderr_content:
                print(f"Stderr for process {process_id}:")
                print(stderr_content)

    decisions = []
    for process_id in range(1, num_processes + 1):
        print(f"Validating process {process_id}...")

        output_path = os.path.join(output_dir, "proc{:02d}.out".format(process_id))
        config_path = os.path.join(config_dir, f"lattice-agreement-{process_id}.config")

        # Load decisions
        with open(output_path, "r") as f:
            process_decisions = [set(map(int, line.split())) for line in f]
            # print(f"  Decisions: {process_decisions}")
            decisions.append(process_decisions)

        # Validate termination
        with open(config_path, "r") as f:
            num_proposals = int(f.readline().split()[0])
            if len(process_decisions) != num_proposals:
                print(f"Process {process_id} failed termination: {len(process_decisions)}/{num_proposals} decisions made.")
                termination_passed = False
        if not termination_passed:
            exit(1)

        # Validate validity
        with open(config_path, "r") as f:
            _ = f.readline()  # Skip the first line
            proposals = [set(map(int, line.split())) for line in f]

        # print(f"  Proposals: {proposals}")

        for i, decision in enumerate(process_decisions):
            if not proposals[i].issubset(decision):
                print(f"Process {process_id} failed validity for proposal {i + 1}.")
                validity_passed = False

    # Validate consistency across processes
    for slot in range(len(decisions[0])):
        slot_decisions = [decisions[pid][slot] for pid in range(num_processes)]
        for i in range(len(slot_decisions)):
            for j in range(i + 1, len(slot_decisions)):
                if not (slot_decisions[i].issubset(slot_decisions[j]) or slot_decisions[j].issubset(slot_decisions[i])):
                    print(f"Consistency violated between processes for slot {slot + 1}.")
                    consistency_passed = False

    # Print summary
    print("Validation Results:")
    print(f"  Validity:    {'PASSED' if validity_passed else 'FAILED'}")
    print(f"  Consistency: {'PASSED' if consistency_passed else 'FAILED'}")
    print(f"  Termination: {'PASSED' if termination_passed else 'FAILED'}")

if __name__ == "__main__":
    # Parameters (adjust as needed)
    NUM_PROCESSES = 3
    NUM_PROPOSALS = 10
    MAX_VALUES = 10
    DISTINCT_VALUES = 100

    HOSTS_FILE = "example/hosts"  # Path to the hosts file
    OUTPUT_DIR = "example/output"
    CONFIG_DIR = "example/auto-config"

    TIMEOUT = 15  # Number of seconds to run each process

    # Steps
    generate_config_files(NUM_PROCESSES, NUM_PROPOSALS, MAX_VALUES, DISTINCT_VALUES, CONFIG_DIR)
    run_processes(NUM_PROCESSES, HOSTS_FILE, OUTPUT_DIR, CONFIG_DIR, TIMEOUT)
    validate_outputs(NUM_PROCESSES, OUTPUT_DIR, CONFIG_DIR)
