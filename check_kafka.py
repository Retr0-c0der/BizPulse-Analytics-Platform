import subprocess
result = subprocess.run(
    ['docker', 'ps', '--format', 'table {{.Names}}\t{{.Ports}}'],
    capture_output=True, text=True
)
print(result.stdout)