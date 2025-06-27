import asyncio
from typing import TypeVar, Any, Optional
from typing_extensions import Awaitable, Callable
import asyncssh
from asyncssh.connection import SSHClientConnection

PORT_BASE = 3000
HOSTS={
1:  "64.130.36.165",
2:  "64.130.36.164",
3:  "64.130.33.171",
4:  "64.130.36.176",
5:  "64.130.33.246",
6:  "64.130.36.167",
7:  "64.130.33.172",
8:  "64.130.36.174",
9:  "64.130.36.181",
10: "64.130.36.177",
}

#cmd = "sudo apt install rustup && rustup toolchain install stable"
#cmd = "git clone https://github.com/alexpyattaev/alpenglow.git && cd alpenglow && cargo build --bin node"
cmd_run = " cd alpenglow && tmux new-session -d -s ag_prototype 'cargo run --bin node --  --config-name node_config_{}.toml'"
#cmd_run = " cd alpenglow && cargo run --bin node --  --config-name node_config_{}.toml"
cmd_kill = "killall -u sol node"


async def run_cmd(conn: SSHClientConnection, tag:str, cmd:str)->bool:

    result = await conn.run(cmd)
    if result.exit_status ==0:
        print(f"""[{tag}] Output:
            {result.stdout}
            """)
        return True
    else:
        print(f"""[{tag}] Output:
            {result.stdout},

            Error:
            {result.stderr}
            """)
        return False


T = TypeVar("T")
async def run_commands(ip: str, host: str, thing_to_execute:Callable[[SSHClientConnection, str], Awaitable[T]]) -> Optional[T]:
    try:
        async with asyncssh.connect(ip, # type: ignore[reportPrivateImportUsage]
            username="sol",
            client_keys=['/home/sol/.ssh/no_pass_key']) as conn:
                return await thing_to_execute(conn, host)


    except (OSError, asyncssh.Error) as e:# type: ignore[reportPrivateImportUsage]
        print(f"[{host}] SSH connection failed: {e}")



async def make_config_files():
    ip_list = "ip_list.csv"
    with open(ip_list, "w") as file:
        for name, ip in HOSTS.items():
            file.write(f"{ip}:{PORT_BASE}\n")

    res = await asyncio.subprocess.create_subprocess_shell(f"cargo run --bin node -- --generate-config-files {ip_list} --config-name node_config")
    code = await res.wait()
    assert (code ==0)
    return "node_config"

async def upload_file(conn, host, local, remote):
    print(f"uploading {local}->{host}:{remote}")
    async with conn.start_sftp_client() as sftp:
        await sftp.put(local, remote)


async def main():
    #await make_config_files()
    #tasks = [run_commands(ip=v, host=str(k), thing_to_execute=lambda conn, host, k=k: upload_file(conn, host, f"node_config_{k-1}.toml", f"/home/sol/alpenglow/node_config_{k-1}.toml") ) for (k,v) in HOSTS.items()]
    tasks = [run_commands(ip=v, host=str(k), thing_to_execute=lambda conn, host, k=k: run_cmd(conn, host, cmd_run.format(k-1)) ) for (k,v) in HOSTS.items()]
    #tasks = [run_commands(ip=v, host=str(k), thing_to_execute=lambda conn, host, k=k: run_cmd(conn, host, cmd_kill) ) for (k,v) in HOSTS.items()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":

    asyncio.run(main())
