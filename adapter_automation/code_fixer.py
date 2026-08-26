"""Post-generation code fixers: hallucination removal, syntax validation"""

import ast
import re
from typing import Tuple, Optional, Callable, List, Dict, Any
import json
from adapter_automation.llm_client import make_llm_client, find_project_root
from pathlib import Path


def extract_code(llm_response: str) -> str:
    """Extract Python code block from LLM response."""
    match = re.search(r"```python\s*(.*?)```", llm_response, re.DOTALL)
    if match:
        return match.group(1).strip()
    return llm_response.strip()

def validate_syntax(code: str) -> Tuple[bool, str]:
    """Parse Python code to check for syntax errors."""
    try:
        ast.parse(code)
        return True, ""
    except SyntaxError as e:
        return False, str(e)


def llm_fix_syntax_error(code: str, error_msg: str, llm_fn: Optional[Callable] = None) -> Optional[str]:
    """
    
    Args:
        code: The broken Python code
        error_msg: The syntax error message
        llm_fn: Optional LLM function (if None, will create a new client with fixer model)
    
    Returns:
        Fixed code or None if fix failed
    """
    # Check if code is severely incomplete (less than 20 lines suggests truncation)
    line_count = code.count('\n')
    if line_count < 20:
        print(f"  ⚠ Code appears truncated ({line_count} lines) - skipping LLM fix, will retry generation")
        return None
    
    if llm_fn is None:
        llm_fn = make_llm_client(find_project_root(Path(__file__)))
    
    prompt = f"""You are a Python syntax error fixer. Fix the syntax error in this code.

## Syntax Error:
{error_msg}

## Broken Code:
```python
{code}
```

## Instructions:
1. Analyze the syntax error
2. Fix ONLY the syntax issue (don't change logic)
3. Common issues:
   - Unterminated strings: add closing quotes
   - Unterminated docstrings: add closing ''' or \"\"\"
   - Missing colons, brackets, parentheses
   - Indentation errors
4. Return ONLY the fixed Python code in a ```python code block
5. Keep all original logic intact
6. If the code is incomplete/truncated, return it as-is (don't try to complete it)

Return the complete fixed code:"""

    system = "You are a Python syntax error fixer. Fix syntax errors while preserving all logic."
    
    try:
        response = llm_fn(prompt, system=system)
        fixed_code = extract_code(response)
        
        # Validate the fix
        is_valid, _ = validate_syntax(fixed_code)
        if is_valid:
            return fixed_code
        else:
            return None
    except Exception as e:
        print(f"  ⚠ LLM syntax fix failed: {e}")
        return None


def _last_import_pos(c: str) -> int:
    pos = 0
    for m in re.finditer(r"^(?:import |from )\S+.*$", c, re.MULTILINE):
        pos = m.end()
    return pos


def _inject_import(c: str, stmt: str) -> str:
    pos = _last_import_pos(c)
    return c[:pos] + f"\n{stmt}" + c[pos:] if pos else f"{stmt}\n" + c


def fix_code_hallucinations(code: str) -> str:
    """Fix common LLM hallucinations in the generated adapter code."""

    def _sanitize_docstring(m):
        content = m.group(1).replace('\\', '\\\\').replace('"""', "'''")
        return f'"""{content}"""'
    code = re.sub(r'"""(.*?)"""', _sanitize_docstring, code, flags=re.DOTALL)

    if "try:" in code and "except" not in code and "finally" not in code:
        try_matches = list(re.finditer(r"^([ \t]*)try:\s*$", code, re.MULTILINE))
        if try_matches:
            last_try = try_matches[-1]
            indent = last_try.group(1)
            rest = code[last_try.end():]
            has_closer = re.search(rf"^{indent}(?:except|finally)\b", rest, re.MULTILINE)
            if not has_closer:
                code += f"\n{indent}except Exception as e:\n{indent}    print(f'Error: {{e}}')\n"

    return code



def extract_json(text: str) -> dict:
    """Robustly extract the first valid JSON object from a string.
    Best for column mapper and general JSON responses.
    """
    # First, try to extract JSON from markdown code blocks
    code_block_pattern = r'```(?:json)?\s*(\{.*\})\s*```'
    code_match = re.search(code_block_pattern, text, re.DOTALL)
    if code_match:
        try:
            return json.loads(code_match.group(1))
        except json.JSONDecodeError:
            pass
    
    # Walk through the text looking for '{' and try to find matching '}'
    search_start = 0
    while True:
        start_idx = text.find('{', search_start)
        if start_idx == -1:
            break
            
        stack = 0
        in_string = False
        escape_next = False
        end_idx = -1
        
        for i in range(start_idx, len(text)):
            char = text[i]
            if escape_next: escape_next = False; continue
            if char == '\\': escape_next = True; continue
            if char == '"': in_string = not in_string; continue
            if not in_string:
                if char == '{': stack += 1
                elif char == '}':
                    stack -= 1
                    if stack == 0: end_idx = i; break
        
        if end_idx != -1:
            candidate = text[start_idx : end_idx + 1]
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass
        
        search_start = start_idx + 1

    # Final fallback: greedy regex search for anything between braces
    json_match = re.search(r'(\{.*\})', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass
            
    return {}

def extract_adapter_json(text: str) -> dict:
    """Specialized JSON extraction for the adapter generator.
    Handles 'reasoning' and 'code' fields specifically.
    """
    # First, try to extract JSON from markdown code blocks
    code_block_pattern = r'```(?:json)?\s*(\{.*\})\s*```'
    code_match = re.search(code_block_pattern, text, re.DOTALL)
    if code_match:
        content = code_match.group(1)
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            text = content
    
    reasoning_match = re.search(r'"reasoning":\s*"(.*?)"(?=,\s*"|\s*\})', text, re.DOTALL)
    code_match = re.search(r'"code":\s*"(.*?)"(?=\s*\})', text, re.DOTALL)
    
    def unescape(s):
        try:
            return json.loads(f'"{s}"')
        except:
            # Fallback if unescaping fails
            return s.replace('\\n', '\n').replace('\\"', '"').replace('\\\\', '\\')

    if reasoning_match and code_match:
        return {
            "reasoning": unescape(reasoning_match.group(1)),
            "code": unescape(code_match.group(1))
        }
    elif code_match:
        return {
            "reasoning": "Extraction fallback (logic only)",
            "code": unescape(code_match.group(1))
        }
    try:
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1:
            candidate = text[start:end+1]
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass
    except:
        pass
            
    return {}
