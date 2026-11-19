"""Run this project's full unittest suite and the linkage checks from the repository root."""
import subprocess
import sys
import unittest


def main():
    suite = unittest.defaultTestLoader.discover('tests', top_level_dir='.')
    if suite.countTestCases():
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    else:
        print('No tests discovered in tests/', file=sys.stderr)
        result = unittest.TestResult()
    for status, failed_cases in (
        ('FAIL', (test for test, _ in result.failures)),
        ('ERROR', (test for test, _ in result.errors)),
        ('UNEXPECTED_SUCCESS', result.unexpectedSuccesses),
    ):
        for test in failed_cases:
            print(f'{status} {test.id()}')
    successful = result.testsRun > 0 and result.wasSuccessful()
    print(f'TEST SUMMARY total={result.testsRun} failures={len(result.failures)} '
          f'errors={len(result.errors)} skipped={len(result.skipped)} '
          f'expected_failures={len(result.expectedFailures)} '
          f'unexpected_successes={len(result.unexpectedSuccesses)} '
          f'result={"PASS" if successful else "FAIL"}')
    linkage = subprocess.run([sys.executable, '-B', 'work_order_cases.py'], check=False)
    if linkage.returncode != 0:
        print(f'LINKAGE CASES FAILED exit={linkage.returncode}')
    return 0 if successful and linkage.returncode == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
