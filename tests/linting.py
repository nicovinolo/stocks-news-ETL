import subprocess

def test_run_lint():
    """Run flake8 linting and assert no errors."""
    result = subprocess.run(
        ["flake8", "--max-line-length=200", "--exclude=env,.git,tests,utils"],
        capture_output=True, text=True
    )
    if result.stdout:
        print("Linting Output:\n", result.stdout)
    if result.stderr:
        print("Linting Errors:\n", result.stderr)
    assert result.returncode == 0, f"Linting failed:\n{result.stdout}\n{result.stderr}"

if __name__ == "__main__":
    test_run_lint()