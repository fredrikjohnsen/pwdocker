from shlex import quote
import typer

from util import run_shell_cmd


def unzip(zipfile, to_dir):
    """ Unzip file with correct encoding for norwegian """
    encoding = None
    for enc in ['IBM850', 'windows-1252']:
        cmd = [f"lsar -e {enc} {quote(zipfile)}"]
        result, out, err = run_shell_cmd(cmd, shell=True)
        if 'æ' in out or 'ø' in out or 'å' in out:
            encoding = enc
            break

    if encoding:
        cmd = [f"unar -k skip -D -e {encoding} {quote(zipfile)} -o {quote(to_dir)}"]
    else:
        cmd = [f"unar -k skip -D {quote(zipfile)} -o {quote(to_dir)}"]

    result, out, err = run_shell_cmd(cmd, shell=True)

    if result:
        print(out)
        raise typer.Exit(code=1)

    return None


if __name__ == '__main__':
    typer.run(unzip)
