#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

workflow {
Channel
        .fromPath(file(params.input))
        .splitCsv(header: true)
        .map{ it -> [ it.workflow, file(it.path).listFiles().toList() ] }
        .transpose()
        .map{ workflow, run -> [ workflow: workflow,
                                 run: run,
                                 timestamp: file(run).lastModified() / 1000,
                                 directory: file(run).isDirectory() ? true : false,
                                 manifest: run.resolve("manifest.csv").exists() ? run.resolve("manifest.csv") : null ] }
        .view()
}
