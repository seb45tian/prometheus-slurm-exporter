/* Copyright 2020 Victor Penso

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
    "io/ioutil"
    "os/exec"
    "log"
	"regexp"
    "strings"
    "strconv"
    "github.com/prometheus/client_golang/prometheus"
)

func PartitionsData() []byte {
    cmd := exec.Command("sinfo", "-h", "-o%R,%D,%C,%F")
    // "%R,%D,%C,%F" == "partion,NNodes,NCPUsPerNode,State"
    stdout, err := cmd.StdoutPipe()
    if err != nil {
            log.Fatal(err)
    }
    if err := cmd.Start(); err != nil {
            log.Fatal(err)
    }
    out, _ := ioutil.ReadAll(stdout)
    if err := cmd.Wait(); err != nil {
            log.Fatal(err)
    }
    return out
}

func PartitionsPendingJobsData() []byte {
    cmd := exec.Command("squeue","-a","-r","-h","-o%P","--states=PENDING")
    stdout, err := cmd.StdoutPipe()
    if err != nil {
            log.Fatal(err)
    }
    if err := cmd.Start(); err != nil {
            log.Fatal(err)
    }
    out, _ := ioutil.ReadAll(stdout)
    if err := cmd.Wait(); err != nil {
            log.Fatal(err)
    }
    return out
}

type PartitionMetrics struct {
    pending float64
	nalloc float64
	ncomp  float64
	ndown  float64
	ndrain float64
	nerr   float64
	nfail  float64
	nidle  float64
	nmaint float64
	nmix   float64
	nresv  float64
	calloc float64
	ccomp  float64
	cdown  float64
	cdrain float64
	cerr   float64
	cfail  float64
	cidle  float64
	cmaint float64
	cmix   float64
	cresv  float64
}

func ParsePartitionsMetrics() map[string]*PartitionMetrics {
    partitions := make(map[string]*PartitionMetrics)
    lines := strings.Split(string(PartitionsData()), "\n")
    for _, line := range lines {
    // "%R,%D,%C,%F" == "partion,NNodes,NCPUsPerNode,State"
        if strings.Contains(line,",") {
            split := strings.Split(line, ",")
            // name of a partition
            partition := split[0]
            _,key := partitions[partition]
            if !key {
                    partitions[partition] = &PartitionMetrics{0,0,0,0,0,0,0,0,
                                                              0,0,0,0,0,0,0,0,
                                                              0,0,0,0,0}
            }

            node_count, _ := strconv.ParseFloat(strings.TrimSpace(split[1]), 64)
            cpu_per_nodes, _ := strconv.ParseFloat(strings.TrimSpace(split[2]), 64)
            cpu_count := node_count * cpu_per_nodes
            state := split[3]

            alloc := regexp.MustCompile(`^alloc`)
            comp := regexp.MustCompile(`^comp`)
            down := regexp.MustCompile(`^down`)
            drain := regexp.MustCompile(`^drain`)
            fail := regexp.MustCompile(`^fail`)
            err := regexp.MustCompile(`^err`)
            idle := regexp.MustCompile(`^idle`)
            maint := regexp.MustCompile(`^maint`)
            mix := regexp.MustCompile(`^mix`)
            resv := regexp.MustCompile(`^res`)
            switch {
            case alloc.MatchString(state) == true:
                partitions[partition].nalloc += node_count
                partitions[partition].calloc += cpu_count
            case comp.MatchString(state) == true:
                partitions[partition].ncomp += node_count
                partitions[partition].ccomp += cpu_count
            case down.MatchString(state) == true:
                partitions[partition].ndown += node_count
                partitions[partition].cdown += cpu_count
            case drain.MatchString(state) == true:
                partitions[partition].ndrain += node_count
                partitions[partition].cdrain += cpu_count
            case fail.MatchString(state) == true:
                partitions[partition].nfail += node_count
                partitions[partition].cfail += cpu_count
            case err.MatchString(state) == true:
                partitions[partition].nerr += node_count
                partitions[partition].cerr += cpu_count
            case idle.MatchString(state) == true:
                partitions[partition].nidle += node_count
                partitions[partition].cidle += cpu_count
            case maint.MatchString(state) == true:
                partitions[partition].nmaint += node_count
                partitions[partition].cmaint += cpu_count
            case mix.MatchString(state) == true:
                partitions[partition].nmix += node_count
                partitions[partition].cmix += cpu_count
            case resv.MatchString(state) == true:
                partitions[partition].nresv += node_count
                partitions[partition].cresv += cpu_count
            }
        }
    }
    // get list of pending jobs by partition name
    list := strings.Split(string(PartitionsPendingJobsData()),"\n")
    for _,partition := range list {
    // accumulate the number of pending jobs
    _,key := partitions[partition]
    if key {
        partitions[partition].pending += 1
            }
    }


    return partitions
}

type PartitionsCollector struct {
    pending *prometheus.Desc
	nalloc *prometheus.Desc
	ncomp  *prometheus.Desc
	ndown  *prometheus.Desc
	ndrain *prometheus.Desc
	nerr   *prometheus.Desc
	nfail  *prometheus.Desc
	nidle  *prometheus.Desc
	nmaint *prometheus.Desc
	nmix   *prometheus.Desc
	nresv  *prometheus.Desc
	calloc *prometheus.Desc
	ccomp  *prometheus.Desc
	cdown  *prometheus.Desc
	cdrain *prometheus.Desc
	cerr   *prometheus.Desc
	cfail  *prometheus.Desc
	cidle  *prometheus.Desc
	cmaint *prometheus.Desc
	cmix   *prometheus.Desc
	cresv  *prometheus.Desc



}

func NewPartitionsCollector() *PartitionsCollector {
    labels := []string{"partition"}
    return &PartitionsCollector{
        pending: prometheus.NewDesc("slurm_partition_jobs_pending", "Pending jobs for partition", labels,nil),
		nalloc: prometheus.NewDesc("slurm_partition_nodes_alloc", "Allocated nodes for partition", labels, nil),
		ncomp:  prometheus.NewDesc("slurm_partition_nodes_comp", "Completing nodes for partition", labels, nil),
		ndown:  prometheus.NewDesc("slurm_partition_nodes_down", "Down nodes for partition", labels, nil),
		ndrain: prometheus.NewDesc("slurm_partition_nodes_drain", "Drain nodes for partition", labels, nil),
		nerr:   prometheus.NewDesc("slurm_partition_nodes_err", "Error nodes for partition", labels, nil),
		nfail:  prometheus.NewDesc("slurm_partition_nodes_fail", "Fail nodes for partition", labels, nil),
		nidle:  prometheus.NewDesc("slurm_partition_nodes_idle", "Idle nodes for partition", labels, nil),
		nmaint: prometheus.NewDesc("slurm_partition_nodes_maint", "Maint nodes for partition", labels, nil),
		nmix:   prometheus.NewDesc("slurm_partition_nodes_mix", "Mix nodes for partition", labels, nil),
		nresv:  prometheus.NewDesc("slurm_partition_nodes_resv", "Reserved nodes for partition", labels, nil),
		calloc: prometheus.NewDesc("slurm_partition_cpus_alloc", "Allocated CPUs for partition", labels, nil),
		ccomp:  prometheus.NewDesc("slurm_partition_cpus_comp", "Completing CPUs for partition", labels, nil),
		cdown:  prometheus.NewDesc("slurm_partition_cpus_down", "Down CPUs for partition", labels, nil),
		cdrain: prometheus.NewDesc("slurm_partition_cpus_drain", "Drain CPUs for partition", labels, nil),
		cerr:   prometheus.NewDesc("slurm_partition_cpus_err", "Error CPUs for partition", labels, nil),
		cfail:  prometheus.NewDesc("slurm_partition_cpus_fail", "Fail CPUs for partition", labels, nil),
		cidle:  prometheus.NewDesc("slurm_partition_cpus_idle", "Idle CPUs for partition", labels, nil),
		cmaint: prometheus.NewDesc("slurm_partition_cpus_maint", "Maint CPUs for partition", labels, nil),
		cmix:   prometheus.NewDesc("slurm_partition_cpus_mix", "Mix CPUs for partition", labels, nil),
		cresv:  prometheus.NewDesc("slurm_partition_cpus_resv", "Reserved CPUs for partition", labels, nil),
    }
}

func (pc *PartitionsCollector) Describe(ch chan<- *prometheus.Desc) {
    ch <- pc.pending
	ch <- pc.nalloc
	ch <- pc.ncomp
	ch <- pc.ndown
	ch <- pc.ndrain
	ch <- pc.nerr
	ch <- pc.nfail
	ch <- pc.nidle
	ch <- pc.nmaint
	ch <- pc.nmix
	ch <- pc.nresv
	ch <- pc.calloc
	ch <- pc.ccomp
	ch <- pc.cdown
	ch <- pc.cdrain
	ch <- pc.cerr
	ch <- pc.cfail
	ch <- pc.cidle
	ch <- pc.cmaint
	ch <- pc.cmix
	ch <- pc.cresv
}

func (pc *PartitionsCollector) Collect(ch chan<- prometheus.Metric) {
    pm := ParsePartitionsMetrics()
    for p := range pm {
        if pm[p].pending > 0 {
            ch <- prometheus.MustNewConstMetric(pc.pending, prometheus.GaugeValue, pm[p].pending, p)
        }
        if pm[p].nalloc > 0 {
            ch <- prometheus.MustNewConstMetric(pc.nalloc, prometheus.GaugeValue, pm[p].nalloc, p)
        }
        if pm[p].ncomp > 0 {
            ch <- prometheus.MustNewConstMetric(pc.ncomp,  prometheus.GaugeValue, pm[p].ncomp, p)
        }
        if pm[p].ndown > 0 {
            ch <- prometheus.MustNewConstMetric(pc.ndown,  prometheus.GaugeValue, pm[p].ndown, p)
        }
        if pm[p].ndrain > 0 {
            ch <- prometheus.MustNewConstMetric(pc.ndrain, prometheus.GaugeValue, pm[p].ndrain, p)
        }
        if pm[p].nerr > 0 {
            ch <- prometheus.MustNewConstMetric(pc.nerr,   prometheus.GaugeValue, pm[p].nerr, p)
        }
        if pm[p].nfail > 0 {
            ch <- prometheus.MustNewConstMetric(pc.nfail,  prometheus.GaugeValue, pm[p].nfail, p)
        }
        if pm[p].nidle > 0 {
            ch <- prometheus.MustNewConstMetric(pc.nidle,  prometheus.GaugeValue, pm[p].nidle, p)
        }
        if pm[p].nmaint > 0 {
            ch <- prometheus.MustNewConstMetric(pc.nmaint, prometheus.GaugeValue, pm[p].nmaint, p)
        }
        if pm[p].nmix > 0 {
            ch <- prometheus.MustNewConstMetric(pc.nmix,   prometheus.GaugeValue, pm[p].nmix, p)
        }
        if pm[p].nresv > 0 {
            ch <- prometheus.MustNewConstMetric(pc.nresv,  prometheus.GaugeValue, pm[p].nresv, p)
        }
        if pm[p].calloc > 0 {
            ch <- prometheus.MustNewConstMetric(pc.calloc, prometheus.GaugeValue, pm[p].calloc, p)
        }
        if pm[p].ccomp > 0 {
            ch <- prometheus.MustNewConstMetric(pc.ccomp,  prometheus.GaugeValue, pm[p].ccomp, p)
        }
        if pm[p].cdown > 0 {
            ch <- prometheus.MustNewConstMetric(pc.cdown,  prometheus.GaugeValue, pm[p].cdown, p)
        }
        if pm[p].cdrain > 0 {
            ch <- prometheus.MustNewConstMetric(pc.cdrain, prometheus.GaugeValue, pm[p].cdrain, p)
        }
        if pm[p].cerr > 0 {
            ch <- prometheus.MustNewConstMetric(pc.cerr,   prometheus.GaugeValue, pm[p].cerr, p)
        }
        if pm[p].cfail > 0 {
            ch <- prometheus.MustNewConstMetric(pc.cfail,  prometheus.GaugeValue, pm[p].cfail, p)
        }
        if pm[p].cidle > 0 {
            ch <- prometheus.MustNewConstMetric(pc.cidle,  prometheus.GaugeValue, pm[p].cidle, p)
        }
        if pm[p].cmaint > 0 {
            ch <- prometheus.MustNewConstMetric(pc.cmaint, prometheus.GaugeValue, pm[p].cmaint, p)
        }
        if pm[p].cmix > 0 {
            ch <- prometheus.MustNewConstMetric(pc.cmix,   prometheus.GaugeValue, pm[p].cmix, p)
        }
        if pm[p].cresv > 0 {
            ch <- prometheus.MustNewConstMetric(pc.cresv,  prometheus.GaugeValue, pm[p].cresv, p)
        }
    }
}
