"""Pillar 1 — pure domain. No IO, no now() (inject Clock), no locks, naive datetimes banned.

If a test of this package needs Mock/patch, the seam has leaked — fix the seam, not the test.
"""
