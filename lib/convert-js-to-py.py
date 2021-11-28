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
    pythonContent = jsContent
    pythonContent = re.sub(r"^ *function", "def", pythonContent, flags=re.M)  # 'function'->'def'
    pythonContent = re.sub(r"\) *{ *$", "):", pythonContent, flags=re.M)  # remove brace after func
    pythonContent = re.sub(r"^} *$", "", pythonContent, flags=re.M)  # remove closing func brace
    pythonContent = re.sub(r"true", "True", pythonContent, flags=re.M)  # convert js to py bool
    pythonContent = re.sub(r"false", "False", pythonContent, flags=re.M)  # convert js to py bool
    pythonContent = re.sub(r"null", "None", pythonContent, flags=re.M)  # convert js null to py
    pythonContent = re.sub("/\\*.*?\\*/", "", pythonContent, flags=re.DOTALL)  # remove  cmnts
    pythonContent = re.sub(r"^//", "#", pythonContent, flags=re.M)  # replace js start cmt with py
    pythonContent = re.sub(r"//.*?\n", "\n", pythonContent, flags=re.M)  # remove other js cmnts
    pyFile.write(pythonContent)
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

    # Generate an mardown snippet text file which lists the functions and their descriptions
    docsContent = ""

    for line in jsContent.splitlines():
        if line.startswith("//") or line.startswith("function"):
            docsContent += f"{line}\n"

            if line.startswith("function"):
                docsContent += "\n"

    docsContent = re.sub(r"\).*{.*$", ")", docsContent, flags=re.M)  # remove brace after func
    docsContent = re.sub(r"^ *function *", "", docsContent, flags=re.M)  # remove 'function'
    pythonContent = re.sub("/\\*.*?\\*/", "", pythonContent, flags=re.DOTALL)  # remove block cmnts
    docsFile.write("```javascript\n")
    docsFile.write(docsContent)
    docsFile.write("```\n")
    print(f"Generated new file: '{docsFile.name}'")
