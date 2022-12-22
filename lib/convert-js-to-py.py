#!/usr/bin/python3
import re
from pprint import pprint


LIBFILE = "masksFakesGeneraters"
PY_IMPORT_LINE = f"{LIBFILE}_py_imports.py"
DOCS_MD_FILE = f"{LIBFILE}_docs_md"


with open(f"{LIBFILE}.js", mode="r") as jsFile, \
     open(f"{LIBFILE}.py", mode="w") as pyFile, \
     open(f"{PY_IMPORT_LINE}", mode="w") as importsFile, \
     open(f"{DOCS_MD_FILE}", mode="w") as docsFile:
    jsContent = jsFile.read()

    # Substitute JS-specific parts of code with Python equivalents and output to .py file
    python_content = jsContent
    python_content = re.sub(r"^ *function", "def", python_content, flags=re.M)  # 'function'->'def'
    python_content = re.sub(r"\) *{ *$", "):", python_content, flags=re.M)  # rmve brace after func
    python_content = re.sub(r"^} *$", "", python_content, flags=re.M)  # remove closing func brace
    python_content = re.sub(r"/\*.*?\*/", "", python_content, flags=re.DOTALL)  # remove blkcmts
    python_content = re.sub(r"^//", "#", python_content, flags=re.M)  # convert js start cmt to py
    python_content = re.sub(r"//[^\n]*", "\n", python_content, flags=re.M)  # remove other js cmnts
    python_content = python_content.replace("true", "True")  # convert js to py bool
    python_content = python_content.replace(r"false", "False")  # convert js to py bool
    python_content = python_content.replace(r"null", "None")  # convert js null to py
    pyFile.write(python_content)
    print(f"Generated new file: '{pyFile.name}'")

    # Generate a text file which names the imports of the fake and mask functions
    importLineText = "from masksFakesGeneraters import "
    matches = re.findall(r"^\s*function\s*(.*)\(.*\)\s*{.*$", jsContent, flags=re.M)
    firstMatch = True

    for match in matches:
        if not firstMatch:
            importLineText += ", "

        importLineText += match
        firstMatch = False

    importLineText += "\n"

    importsFile.write(importLineText)
    print(f"Generated new file: '{importsFile.name}'")

    # Generate a mardown snippet text file which lists the functions and their descriptions
    docsContent = ""

    for line in jsContent.splitlines():
        if line.startswith("//") or line.startswith("function"):
            docsContent += f"{line}\n"

            if line.startswith("function"):
                docsContent += "\n"

    docsContent = re.sub(r"\).*{.*$", ")", docsContent, flags=re.M)  # remove brace after func
    docsContent = re.sub(r"^ *function *", "", docsContent, flags=re.M)  # remove 'function'
    python_content = re.sub("/\\*.*?\\*/", "", python_content, flags=re.DOTALL)  # rmve block cmnts
    docsFile.write("```javascript\n")
    docsFile.write(docsContent)
    docsFile.write("```\n")
    print(f"Generated new file: '{docsFile.name}'")
