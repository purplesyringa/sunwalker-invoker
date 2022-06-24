import asyncio
import msgpack
import random
import websockets


PACKAGES = {
    "problems/mock-problem/rev12"
}


MANIFEST = f"""
judging judging.msgpack
programs/
programs/checker/
programs/checker/artifacts/
+x checker programs/checker/artifacts/checker
tests/
""".strip() + "".join(f"\nt{i} tests/{i}\na{i} tests/{i}.a" for i in range(1, 1001))

FILES = {
    "manifest/problems/mock-problem/rev12": MANIFEST.encode(),
    "judging": msgpack.packb([
        [  # dependency_dag
            {  # dependents_of
                i: [i + 1] if i != 1000 else [] for i in range(1, 1001)
            }
        ],

        [  # strategy_factory
            # file %output %stderr %checker_output %checker_stderr
            # tactic user
            #     ro $test as input.txt
            #     rw %output as output.txt
            #     user <input.txt >output.txt 2>%stderr
            # tactic testlib
            #     checker $test %output $test.a >%checker_output 2>%checker_stderr

            {  # files
                "output": "Regular",
                "stderr": "Regular",
                "checker_output": "Regular",
                "checker_stderr": "Regular"
            },

            [  # blocks
                [
                    "User",  # tactic
                    {  # bindings
                        "input.txt": [
                            True,  # readable
                            False,  # writable
                            {"VariableText": "$test"}  # source
                        ],
                        "output.txt": [
                            True,  # readable
                            True,  # writable
                            {"File": "output"}  # source
                        ]
                    },
                    "user",  # command
                    [],  # argv
                    {"VariableText": "input.txt"},  # stdin
                    {"VariableText": "output.txt"},  # stdout
                    {"File": "stderr"}  # stderr
                ],
                [
                    "Testlib",  # tactic
                    {},  # bindings
                    "checker",  # command
                    [  # argv
                        {"VariableText": "$test"},
                        {"File": "output"},
                        {"VariableText": "$test.a"}
                    ],
                    None,  #  stdin
                    {"File": "checker_output"},  # stdout
                    {"File": "checker_stderr"}  # stderr
                ]
            ],

            {  # programs
                "checker": [
                    "gcc",  # package
                    ["checker"],  # prerequisites
                    ["./checker"]  # argv
                ]
            },

            ""  # root
        ],

        [  # data
            "mock-problem",  # problem_id
            "rev12"  # revision_id
        ]
    ]),

    "checker": open("testlib/a.out", "rb").read()
}

for i in range(1, 1001):
    random.seed(i)
    a = random.randint(1, 10 ** 9)
    b = random.randint(1, 10 ** 9)
    FILES[f"t{i}"] = f"{a} {b}\n".encode()
    FILES[f"a{i}"] = f"{a + b}\n".encode()


async def echo(websocket):
    print("New connection from", websocket)

    handshake = None
    cores = set()
    ram = 0
    i = 0

    async for message in websocket:
        message = msgpack.unpackb(message, raw=False)

        if handshake is None:
            if "Handshake" in message:
                invoker_name, = message["Handshake"]
                handshake = {
                    "invoker_name": invoker_name
                }
                print("Handshake:", handshake)
            else:
                print("Expected handshake as the first message, got", message)
                break
        else:
            print(message)

            if "UpdateMode" in message:
                added_cores, removed_cores, designated_ram = message["UpdateMode"]
                cores |= set(added_cores)
                cores -= set(removed_cores)
                ram = designated_ram

                for core in added_cores:
                    submission_id = f"mock-{i}-{core}"

                    await websocket.send(msgpack.packb({
                        "AddSubmission": [
                            core,  # compilation_core
                            submission_id,  # submission_id
                            "mock-problem",  # problem_id
                            "rev12",  # revision_id
                            {  # files
                                "hello.cpp": list("""
#include <bits/stdc++.h>
#warning "hello there"
int main() {
    int a, b;
    std::cin >> a >> b;
    std::cout << a + b << std::endl;
}
""".encode())
                            },
                            "cxx.20.gcc"  # language
                        ]
                    }))

                    i += 1
            elif "NotifyCompilationStatus" in message:
                submission_id, success, log = message["NotifyCompilationStatus"]

                core = int(submission_id.split("-")[-1])

                await websocket.send(msgpack.packb({
                    "PushToJudgementQueue": [
                        core,  # core
                        submission_id,  # submission_id
                        list(range(1, 1001)),  # tests
                    ]
                }))
            elif "RequestFile" in message:
                request_id, file_hash = message["RequestFile"]

                await websocket.send(msgpack.packb({
                    "SupplyFile": [
                        request_id,  # request_id
                        list(FILES[file_hash]),  # contents
                    ]
                }))


    print("Closed connection with", websocket)


async def main():
    async with websockets.serve(echo, "127.0.0.1", 8081):
        await asyncio.Future()  # run forever


asyncio.run(main())
