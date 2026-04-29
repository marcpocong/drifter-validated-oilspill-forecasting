"""
Drifter-Validated Oil Spill Forecasting System
A thesis project for particle tracking and validation of oil spill forecasts.
"""
try:
    import matplotlib
except ModuleNotFoundError:
    matplotlib = None
else:
    matplotlib.use("Agg")  # Set headless backend once for the entire process.

__version__ = "0.1.0"
__author__ = "kkreiju"
__description__ = "Oil spill forecasting system using OpenDrift and PyGNOME validation"
