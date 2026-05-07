import logging
import os
import random
import time
from typing import Dict, List, Optional

from google import genai
from google.genai import errors

from Job_pipeline.preprocessing.gemini_key_selector import get_all_gemini_api_keys

logger = logging.getLogger(__name__)

class RobustGeminiClient:
    """
    A robust client for calling Gemini API with multiple keys, rotating them automatically
    on 429 Too Many Requests, and applying exponential backoff.
    """

    def __init__(self, model: Optional[str] = None):
        self.model = model or os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
        self.keys = get_all_gemini_api_keys()
        if not self.keys:
            logger.warning("No Gemini API keys found. Gemini fallback will fail immediately.")
            
        # State tracking: key -> timestamp when it becomes available again
        self.key_backoffs: Dict[str, float] = {k: 0.0 for k in self.keys}
        
        # We can also keep track of consecutive failures per key to back off more
        self.key_failures: Dict[str, int] = {k: 0 for k in self.keys}

    def _get_available_key(self) -> Optional[str]:
        now = time.time()
        available = [k for k in self.keys if self.key_backoffs[k] <= now]
        if not available:
            return None
        return random.choice(available)
        
    def _get_soonest_available_time(self) -> float:
        if not self.keys:
            return time.time()
        return min(self.key_backoffs.values())

    def _mark_key_failed(self, key: str, status_code: Optional[int] = None):
        self.key_failures[key] += 1
        failures = self.key_failures[key]
        
        # Exponential backoff for the specific key. 
        # Base backoff depends on if it's a 429 or general failure.
        base_delay = 60 if status_code == 429 else 10
        
        # Add jitter
        jitter = random.uniform(0.8, 1.2)
        delay = base_delay * (2 ** (min(failures - 1, 5))) * jitter
        
        # Cap max delay to 10 minutes
        delay = min(delay, 600)
        
        self.key_backoffs[key] = time.time() + delay
        logger.warning(
            f"Key ending in ...{key[-4:]} failed (status={status_code}). "
            f"Backing off for {delay:.2f}s (failure count: {failures})"
        )

    def _mark_key_success(self, key: str):
        if self.key_failures[key] > 0:
            self.key_failures[key] = max(0, self.key_failures[key] - 1)

    def __call__(self, prompt: str) -> Optional[str]:
        if not self.keys:
            return None

        max_attempts = 10
        attempt = 0
        
        while attempt < max_attempts:
            key = self._get_available_key()
            if not key:
                now = time.time()
                soonest = self._get_soonest_available_time()
                sleep_time = max(0.1, soonest - now)
                
                # Cap the sleep so we don't hang forever without checking
                sleep_time = min(sleep_time, 10.0)
                
                logger.info(f"All keys rate limited. Sleeping for {sleep_time:.2f}s...")
                time.sleep(sleep_time)
                continue
                
            attempt += 1
            
            try:
                client = genai.Client(api_key=key)
                response = client.models.generate_content(
                    model=self.model,
                    contents=prompt
                )
                
                # Success!
                self._mark_key_success(key)
                
                text = str(getattr(response, "text", "") or "").strip()
                return text
                
            except errors.APIError as e:
                err_str = str(e).lower()
                status_code = getattr(e, "status", None)
                if status_code is None:
                    # try to extract from message
                    if "429" in err_str:
                        status_code = 429
                    elif "500" in err_str:
                        status_code = 500
                    elif "503" in err_str:
                        status_code = 503
                
                if status_code == 429 or "too many requests" in err_str or "quota" in err_str:
                    logger.info(f"Rate limited (429) on key ...{key[-4:]}")
                    self._mark_key_failed(key, status_code=429)
                elif status_code in (500, 502, 503, 504):
                    logger.warning(f"Server error ({status_code}) on key ...{key[-4:]}")
                    self._mark_key_failed(key, status_code=status_code)
                else:
                    logger.error(f"Unrecoverable API error: {e}")
                    # Unrecoverable error, probably bad request
                    return None
                    
            except Exception as e:
                err_str = str(e).lower()
                if "429" in err_str or "too many requests" in err_str:
                    self._mark_key_failed(key, status_code=429)
                else:
                    logger.exception(f"Unexpected error calling Gemini API with key ...{key[-4:]}: {e}")
                    self._mark_key_failed(key)
                
        logger.error(f"Failed to generate content after {max_attempts} attempts.")
        return None

