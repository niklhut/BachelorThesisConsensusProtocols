package raft.app

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.core.subcommands

/**
 * Main application class for the CLI.
 */
class RaftApp : CliktCommand(
    name = "raft",
) {
    override fun run() = Unit
}

/**
 * Main function to run the CLI application.
 */
fun main(args: Array<String>) {
    RaftApp()
        .subcommands(PeerCommand())
        .main(args)
}