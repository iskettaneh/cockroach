#!/usr/bin/env python3
import re
import sys

def extract_durations_from_stdin():
    """Extract durations from log lines read from stdin and format them as requested."""
    
    # Pattern to match durations in seconds and milliseconds from full log lines
    log_pattern = r'Recovery completed after ([\d.]+)(ms|s)'
    # Pattern to match durations that are already formatted (e.g., "5.60s")
    duration_pattern = r'^([\d.]+)s$'
    
    durations = []
    
    for line in sys.stdin:
        line = line.strip()
        
        # Try to match full log line format first
        match = re.search(log_pattern, line)
        if match:
            value = float(match.group(1))
            unit = match.group(2)
            
            # Skip millisecond durations as requested
            if unit == 'ms':
                continue
            
            # Store the numeric value for sorting
            durations.append(value)
            continue
        
        # Try to match already formatted duration (e.g., "5.60s")
        match = re.search(duration_pattern, line)
        if match:
            value = float(match.group(1))
            durations.append(value)
    
    # Sort durations from smaller to larger
    durations.sort()
    
    # Format and print sorted durations
    for duration in durations:
        formatted_duration = f"{duration:.2f}s"
        print(formatted_duration)

if __name__ == "__main__":
    extract_durations_from_stdin()