/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These specs run the backup task against real PostgreSQL and MySQL servers in
// the restricted-PSS e2e namespace, using the operator's default dump images
// and scripts. Migrate/init run a DB client image with a command that mutates
// a marker row, so each dump's contents prove it was taken before the mutation
// it guards.

var (
	postgresImage = getEnvOrDefault("E2E_POSTGRES_IMAGE", "postgres:17-alpine")
	mysqlImage    = getEnvOrDefault("E2E_MYSQL_IMAGE", "mysql:8.4")
)

const backupTimeout = 5 * time.Minute

const (
	firstRunEnvPath = "{.spec.template.spec.containers[0].env" +
		"[?(@.name=='SUPERSET_OPERATOR__BACKUP_FIRST_RUN')].value}"
	dbPassRefEnvPath = "{.spec.template.spec.containers[0].env" +
		"[?(@.name=='SUPERSET_OPERATOR__DB_PASS')].valueFrom.secretKeyRef.name}"
)

// restrictedSecurityYAML renders a restricted-PSS pod + container security
// context for the given UID.
func restrictedSecurityYAML(indent string, uid int) string {
	return fmt.Sprintf(`%[1]ssecurityContext:
%[1]s  runAsNonRoot: true
%[1]s  runAsUser: %[2]d
%[1]s  runAsGroup: %[2]d
%[1]s  fsGroup: %[2]d
%[1]s  seccompProfile:
%[1]s    type: RuntimeDefault
`, indent, uid)
}

const restrictedContainerSecurityYAML = `        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop:
            - ALL
`

// runVerifierPod runs a one-shot Pod that mounts the backup PVC at /backup,
// waits for it to finish, and returns its logs.
func runVerifierPod(name, image, pvc string, uid int, script string) string {
	return runPVCPod(name, image, pvc, uid, "", script)
}

// runPVCPod is runVerifierPod with extra container env (YAML list items
// indented for the container's env field).
func runPVCPod(name, image, pvc string, uid int, env, script string) string {
	envYAML := ""
	if env != "" {
		envYAML = "    env:\n" + env
	}
	pod := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  restartPolicy: Never
%[5]s  containers:
  - name: verify
    image: %[3]s
    command: ["/bin/sh", "-c", %[6]q]
%[8]s%[7]s    volumeMounts:
    - name: backups
      mountPath: /backup
  volumes:
  - name: backups
    persistentVolumeClaim:
      claimName: %[4]s
`, name, namespace, image, pvc, restrictedSecurityYAML("  ", uid), script,
		strings.ReplaceAll(restrictedContainerSecurityYAML, "        ", "    "), envYAML)
	_, _ = runKubectl("delete", "pod", name, "-n", namespace, "--ignore-not-found", "--wait=true")
	applyYAML(name, pod)
	EventuallyWithOffset(1, func(g Gomega) {
		phase, err := jsonPath("pod", name, "{.status.phase}")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(phase).To(BeElementOf("Succeeded", "Failed"))
	}, backupTimeout, 2*time.Second).Should(Succeed())
	logs, err := runKubectl("logs", name, "-n", namespace)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	phase, _ := jsonPath("pod", name, "{.status.phase}")
	ExpectWithOffset(1, phase).To(Equal("Succeeded"), "verifier %s failed:\n%s", name, logs)
	_, _ = runKubectl("delete", "pod", name, "-n", namespace, "--ignore-not-found")
	return logs
}

// creationTime returns a resource's metadata.creationTimestamp.
func creationTime(resource, name string) time.Time {
	out, err := jsonPath(resource, name, "{.metadata.creationTimestamp}")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(out))
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return t
}

var _ = Describe("Superset lifecycle backup (PostgreSQL)", Ordered, func() {
	const crName = "test-backup-pg"
	const pgName = crName + "-db"
	const pvcName = crName + "-backups"
	const backupJob = crName + "-backup"
	pgUID := 70

	psql := func(sql string) string {
		out, err := runKubectl("exec", "-n", namespace, "deploy/"+pgName, "--",
			"psql", "-X", "-U", "superset", "-d", "superset", "-tA", "-v", "ON_ERROR_STOP=1", "-c", sql)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		return strings.TrimSpace(out)
	}

	// migrateCommand sets the marker row to value when migrate runs.
	migrateCommand := func(value string) string {
		return fmt.Sprintf(`["/bin/sh", "-c", "PGPASSWORD=\"$SUPERSET_OPERATOR__DB_PASS\" psql -X -v ON_ERROR_STOP=1 `+
			`-h \"$SUPERSET_OPERATOR__DB_HOST\" -U \"$SUPERSET_OPERATOR__DB_USER\" -d \"$SUPERSET_OPERATOR__DB_NAME\" `+
			`-c \"UPDATE marker SET v = '%s'\""]`, value)
	}

	// dumps lists completed dumps and prints the marker value captured by the
	// newest one.
	dumps := func() (files []string, newestMarker string) {
		logs := runVerifierPod(crName+"-verify", postgresImage, pvcName, pgUID, `set -eu
cd /backup/dumps
if ls *.partial >/dev/null 2>&1; then echo "PARTIAL_LEFT"; fi
for f in $(ls -1 *.dump | sort); do
  echo "FILE $f"; echo "MODE $(stat -c %a "$f")"; pg_restore --list "$f" >/dev/null
done
NEWEST=$(ls -1 *.dump | sort | tail -n 1)
echo "MARKER $(pg_restore -a -t marker -f - "$NEWEST" | sed -n '/^COPY/{n;p;}')"`)
		Expect(logs).NotTo(ContainSubstring("PARTIAL_LEFT"), "no .partial files may remain")
		for line := range strings.SplitSeq(logs, "\n") {
			if f, ok := strings.CutPrefix(line, "FILE "); ok {
				files = append(files, f)
			}
			if mode, ok := strings.CutPrefix(line, "MODE "); ok {
				Expect(strings.TrimSpace(mode)).To(Equal("600"), "dumps must be owner-only")
			}
			if m, ok := strings.CutPrefix(line, "MARKER "); ok {
				newestMarker = strings.TrimSpace(m)
			}
		}
		return files, newestMarker
	}

	BeforeAll(func() {
		DeferCleanup(func() {
			deleteSuperset(crName)
			_, _ = runKubectl("delete", "deploy,svc,pvc,secret", "-n", namespace,
				pgName, pvcName, crName+"-app", "--ignore-not-found")
			_, _ = runKubectl("delete", "secret", pgName, "-n", namespace, "--ignore-not-found")
		})

		By("deploying PostgreSQL, a backup PVC, and credentials")
		applyYAML(pgName, fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %[1]s
  namespace: %[2]s
stringData:
  password: superset-e2e
---
apiVersion: v1
kind: Secret
metadata:
  name: %[5]s-app
  namespace: %[2]s
stringData:
  secret-key: e2e-secret-key-not-for-production
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %[4]s
  namespace: %[2]s
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  selector:
    app: %[1]s
  ports:
  - port: 5432
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %[1]s
  template:
    metadata:
      labels:
        app: %[1]s
    spec:
%[6]s      containers:
      - name: postgres
        image: %[3]s
        env:
        - name: POSTGRES_USER
          value: superset
        - name: POSTGRES_DB
          value: superset
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: %[1]s
              key: password
        - name: PGDATA
          value: /var/lib/postgresql/data/pgdata
        ports:
        - containerPort: 5432
        readinessProbe:
          exec:
            command: ["pg_isready", "-U", "superset", "-d", "superset", "-h", "127.0.0.1"]
          periodSeconds: 2
%[7]s        volumeMounts:
        - name: data
          mountPath: /var/lib/postgresql/data
        - name: run
          mountPath: /var/run/postgresql
      volumes:
      - name: data
        emptyDir: {}
      - name: run
        emptyDir: {}
`, pgName, namespace, postgresImage, pvcName, crName,
			restrictedSecurityYAML("      ", pgUID), restrictedContainerSecurityYAML))
		_, err := runKubectl("rollout", "status", "deploy/"+pgName, "-n", namespace, "--timeout=5m")
		Expect(err).NotTo(HaveOccurred())

		By("writing the pre-install marker row")
		psql("CREATE TABLE marker (v text); INSERT INTO marker VALUES ('v1')")

		By("applying a Production Superset with backup, a web server, and a mutating migrate")
		curlRepo, curlTag := splitImageRef(curlImage)
		pgRepo, pgTag := splitImageRef(postgresImage)
		applyYAML(crName, fmt.Sprintf(`apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  image:
    repository: apache/superset
    tag: "6.1.0"
  environment: Production
  secretKeyFrom:
    name: %[1]s-app
    key: secret-key
  metastore:
    host: %[3]s
    database: superset
    username: superset
    passwordFrom:
      name: %[3]s
      key: password
  webServer:
    image:
      repository: %[4]s
      tag: "%[5]s"
      pullPolicy: IfNotPresent
    podTemplate:
      podSecurityContext:
        runAsNonRoot: true
        seccompProfile:
          type: RuntimeDefault
      container:
        command: ["sleep", "3600"]
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop: [ALL]
          runAsUser: 1000
        livenessProbe:
          exec:
            command: ["true"]
        readinessProbe:
          exec:
            command: ["true"]
        startupProbe:
          exec:
            command: ["true"]
  lifecycle:
    image:
      repository: %[6]s
      tag: "%[7]s"
%[8]s    backup:
      maxRetries: 1
      podRetention:
        policy: Retain
      destination:
        persistentVolumeClaim:
          claimName: %[9]s
          subPath: dumps
    migrate:
      maxRetries: 1
      timeout: 2m
      command: %[10]s
    init:
      command: ["/bin/sh", "-c", "true"]
`, crName, namespace, pgName, curlRepo, curlTag, pgRepo, pgTag,
			restrictedLifecyclePodTemplateYAML(), pvcName, migrateCommand("v2")))
	})

	It("takes a first-run backup before the initial migrate", func() {
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", backupTimeout)
		expectJSONPath("superset", crName, "{.status.lifecycle.backup.state}", "Complete", time.Minute)
		expectJSONPath("job", backupJob,
			firstRunEnvPath, "true", time.Minute)
		expectJSONPath("job", backupJob,
			dbPassRefEnvPath, pgName, time.Minute)
		Expect(psql("SELECT v FROM marker")).To(Equal("v2"))

		files, marker := dumps()
		Expect(files).To(HaveLen(1))
		Expect(files[0]).To(HaveSuffix("_initial.dump"))
		Expect(marker).To(Equal("v1"), "the dump predates the migrate that wrote v2")
	})

	It("runs the backup Job with restricted-PSS-compatible defaults", func() {
		expectJSONPath("job", backupJob, "{.spec.template.spec.containers[0].securityContext.runAsUser}", "70", time.Minute)
		expectJSONPath("job", backupJob,
			"{.spec.template.spec.containers[0].securityContext.runAsNonRoot}", "true", time.Minute)
		expectJSONPath("job", backupJob, "{.spec.template.spec.securityContext.fsGroup}", "70", time.Minute)
		expectJSONPath("job", backupJob, "{.spec.template.spec.containers[0].image}", "postgres:17-alpine", time.Minute)
		expectJSONPath("job", backupJob, "{.spec.activeDeadlineSeconds}", "3600", time.Minute)
	})

	It("drains the web server, then snapshots again before the next migrate", func() {
		expectResourceExists("deployment", crName+"-web-server", 2*time.Minute)
		firstBackup, err := jsonPath("superset", crName, "{.status.lifecycle.backup.completedChecksum}")
		Expect(err).NotTo(HaveOccurred())

		patchSuperset(crName, "merge", fmt.Sprintf(`{"spec":{"lifecycle":{"migrate":{"command":%s}}}}`, migrateCommand("v3")))

		Eventually(func(g Gomega) {
			g.Expect(psql("SELECT v FROM marker")).To(Equal("v3"))
		}, backupTimeout, 2*time.Second).Should(Succeed())
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", backupTimeout)
		second, err := jsonPath("superset", crName, "{.status.lifecycle.backup.completedChecksum}")
		Expect(err).NotTo(HaveOccurred())
		Expect(second).NotTo(Equal(firstBackup))
		expectJSONPath("job", backupJob,
			firstRunEnvPath, "false", time.Minute)

		files, marker := dumps()
		Expect(files).To(HaveLen(2))
		Expect(files[1]).To(HaveSuffix("_17-alpine.dump"))
		Expect(marker).To(Equal("v2"))

		// The web server is only recreated after the whole pipeline, so a
		// Deployment newer than the backup Job proves it was drained first.
		Expect(creationTime("deployment", crName+"-web-server")).
			To(BeTemporally(">=", creationTime("job", backupJob)), "web server must be drained before the backup")
		events, err := runKubectl("get", "events", "-n", namespace,
			"--field-selector", "involvedObject.kind=Superset,involvedObject.name="+crName,
			"-o", "jsonpath={.items[*].reason}")
		Expect(err).NotTo(HaveOccurred())
		Expect(events).To(ContainSubstring("DrainingCompleted"))
	})

	It("does not back up on config-only changes", func() {
		before, err := jsonPath("superset", crName, "{.status.lifecycle.backup.completedChecksum}")
		Expect(err).NotTo(HaveOccurred())
		initBefore, err := jsonPath("superset", crName, "{.status.lifecycle.init.completedChecksum}")
		Expect(err).NotTo(HaveOccurred())

		patchSuperset(crName, "merge", `{"spec":{"config":"E2E_BACKUP_CONFIG_ONLY = True"}}`)

		Eventually(func(g Gomega) {
			initAfter, err := jsonPath("superset", crName, "{.status.lifecycle.init.completedChecksum}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(initAfter).NotTo(Equal(initBefore))
		}, backupTimeout, 2*time.Second).Should(Succeed())
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", backupTimeout)
		expectJSONPath("superset", crName, "{.status.lifecycle.backup.completedChecksum}", before, time.Minute)
		files, _ := dumps()
		Expect(files).To(HaveLen(2))
	})

	It("does not redo a completed backup when only its spec changes", func() {
		before, err := jsonPath("superset", crName, "{.status.lifecycle.backup.completedChecksum}")
		Expect(err).NotTo(HaveOccurred())
		patchSuperset(crName, "merge", `{"spec":{"lifecycle":{"backup":{"timeout":"30m"}}}}`)
		Consistently(func(g Gomega) {
			g.Expect(jsonPath("superset", crName, "{.status.lifecycle.backup.completedChecksum}")).To(Equal(before))
			g.Expect(jsonPath("superset", crName, "{.status.lifecycle.phase}")).To(Equal("Complete"))
		}, 20*time.Second, 2*time.Second).Should(Succeed())
	})

	It("blocks migrate when the backup fails and recovers once it is fixed", func() {
		patchSuperset(crName, "merge",
			`{"spec":{"lifecycle":{"backup":{"command":["/bin/sh","-c","echo simulated dump failure >&2; exit 1"]}}}}`)
		patchSuperset(crName, "merge", fmt.Sprintf(`{"spec":{"lifecycle":{"migrate":{"command":%s}}}}`, migrateCommand("v4")))

		expectJSONPath("superset", crName, "{.status.lifecycle.backup.state}", "Failed", backupTimeout)
		expectJSONPath("superset", crName,
			"{.status.conditions[?(@.type=='LifecycleComplete')].reason}", "TaskFailed", time.Minute)
		Consistently(func() string {
			return psql("SELECT v FROM marker")
		}, 15*time.Second, 3*time.Second).Should(Equal("v3"), "migrate must not run after a failed backup")
		expectResourceGone("deployment", crName+"-web-server")

		By("removing the broken command, which changes the backup pod spec and retries it")
		patchSuperset(crName, "json", `[{"op":"remove","path":"/spec/lifecycle/backup/command"}]`)
		Eventually(func(g Gomega) {
			g.Expect(psql("SELECT v FROM marker")).To(Equal("v4"))
		}, backupTimeout, 2*time.Second).Should(Succeed())
		expectJSONPath("superset", crName, "{.status.lifecycle.backup.state}", "Complete", time.Minute)
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", backupTimeout)

		files, marker := dumps()
		Expect(files).To(HaveLen(3))
		Expect(marker).To(Equal("v3"))
	})

	It("reuses the snapshot when a failed migrate is retried", func() {
		patchSuperset(crName, "merge", `{"spec":{"lifecycle":{"migrate":{"command":["/bin/sh","-c","exit 1"]}}}}`)
		expectJSONPath("superset", crName, "{.status.lifecycle.migrate.state}", "Failed", backupTimeout)
		afterFirst, err := jsonPath("superset", crName, "{.status.lifecycle.backup.completedChecksum}")
		Expect(err).NotTo(HaveOccurred())
		files, _ := dumps()
		Expect(files).To(HaveLen(4))

		patchSuperset(crName, "merge", fmt.Sprintf(`{"spec":{"lifecycle":{"migrate":{"command":%s}}}}`, migrateCommand("v5")))
		Eventually(func(g Gomega) {
			g.Expect(psql("SELECT v FROM marker")).To(Equal("v5"))
		}, backupTimeout, 2*time.Second).Should(Succeed())
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", backupTimeout)
		expectJSONPath("superset", crName, "{.status.lifecycle.backup.completedChecksum}", afterFirst, time.Minute)
		files, marker := dumps()
		Expect(files).To(HaveLen(4), "the retry must not take another snapshot")
		Expect(marker).To(Equal("v4"))
	})
})

var _ = Describe("Superset lifecycle backup (MySQL)", Ordered, func() {
	const crName = "test-backup-mysql"
	const dbName = crName + "-db"
	const pvcName = crName + "-backups"
	mysqlUID := 999

	mysqlExec := func(sql string) string {
		out, err := runKubectl("exec", "-n", namespace, "deploy/"+dbName, "--",
			"mysql", "-usuperset", "-psuperset-e2e", "-N", "-B", "superset", "-e", sql)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		lines := strings.Split(strings.TrimSpace(out), "\n")
		return strings.TrimSpace(lines[len(lines)-1])
	}

	migrateCommand := func(value string) string {
		return fmt.Sprintf(`["/bin/sh", "-c", "MYSQL_PWD=\"$SUPERSET_OPERATOR__DB_PASS\" mysql `+
			`-h \"$SUPERSET_OPERATOR__DB_HOST\" -P \"$SUPERSET_OPERATOR__DB_PORT\" -u \"$SUPERSET_OPERATOR__DB_USER\" `+
			`\"$SUPERSET_OPERATOR__DB_NAME\" -e \"UPDATE marker SET v = '%s'\""]`, value)
	}

	dumps := func() (files []string, newest string) {
		logs := runVerifierPod(crName+"-verify", mysqlImage, pvcName, mysqlUID, `set -eu
cd /backup
if ls *.partial >/dev/null 2>&1; then echo "PARTIAL_LEFT"; fi
for f in $(ls -1 *.sql | sort); do
  echo "FILE $f"; echo "MODE $(stat -c %a "$f")"; tail -n 1 "$f" | grep -q '^-- Dump completed'
done
NEWEST=$(ls -1 *.sql | sort | tail -n 1)
echo "INSERT $(grep 'INSERT INTO .marker.' "$NEWEST")"`)
		Expect(logs).NotTo(ContainSubstring("PARTIAL_LEFT"))
		for line := range strings.SplitSeq(logs, "\n") {
			if f, ok := strings.CutPrefix(line, "FILE "); ok {
				files = append(files, f)
			}
			if mode, ok := strings.CutPrefix(line, "MODE "); ok {
				Expect(strings.TrimSpace(mode)).To(Equal("600"), "dumps must be owner-only")
			}
			if m, ok := strings.CutPrefix(line, "INSERT "); ok {
				newest = m
			}
		}
		return files, newest
	}

	BeforeAll(func() {
		DeferCleanup(func() {
			deleteSuperset(crName)
			_, _ = runKubectl("delete", "deploy,svc,pvc", "-n", namespace, dbName, pvcName, "--ignore-not-found")
			_, _ = runKubectl("delete", "secret", dbName, crName+"-app", "-n", namespace, "--ignore-not-found")
		})

		By("deploying MySQL and a backup PVC")
		applyYAML(dbName, fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %[1]s
  namespace: %[2]s
stringData:
  password: superset-e2e
  root-password: root-e2e
---
apiVersion: v1
kind: Secret
metadata:
  name: %[5]s-app
  namespace: %[2]s
stringData:
  secret-key: e2e-secret-key-not-for-production
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %[4]s
  namespace: %[2]s
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  selector:
    app: %[1]s
  ports:
  - port: 3306
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %[1]s
  template:
    metadata:
      labels:
        app: %[1]s
    spec:
%[6]s      containers:
      - name: mysql
        image: %[3]s
        env:
        - name: MYSQL_DATABASE
          value: superset
        - name: MYSQL_USER
          value: superset
        - name: MYSQL_PASSWORD
          valueFrom:
            secretKeyRef:
              name: %[1]s
              key: password
        - name: MYSQL_ROOT_PASSWORD
          valueFrom:
            secretKeyRef:
              name: %[1]s
              key: root-password
        ports:
        - containerPort: 3306
        readinessProbe:
          exec:
            command: ["mysql", "-h", "127.0.0.1", "-usuperset", "-psuperset-e2e", "-e", "SELECT 1"]
          periodSeconds: 3
%[7]s        volumeMounts:
        - name: data
          mountPath: /var/lib/mysql
        - name: run
          mountPath: /var/run/mysqld
      volumes:
      - name: data
        emptyDir: {}
      - name: run
        emptyDir: {}
`, dbName, namespace, mysqlImage, pvcName, crName,
			restrictedSecurityYAML("      ", mysqlUID), restrictedContainerSecurityYAML))
		_, err := runKubectl("rollout", "status", "deploy/"+dbName, "-n", namespace, "--timeout=5m")
		Expect(err).NotTo(HaveOccurred())

		By("writing the pre-install marker row")
		mysqlExec("CREATE TABLE marker (v varchar(16)); INSERT INTO marker VALUES ('v1')")

		mysqlRepo, mysqlTag := splitImageRef(mysqlImage)
		applyYAML(crName, fmt.Sprintf(`apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  image:
    repository: apache/superset
    tag: "6.1.0"
  environment: Production
  secretKeyFrom:
    name: %[1]s-app
    key: secret-key
  metastore:
    type: MySQL
    host: %[3]s
    database: superset
    username: superset
    passwordFrom:
      name: %[3]s
      key: password
  lifecycle:
    image:
      repository: %[4]s
      tag: "%[5]s"
%[6]s    backup:
      maxRetries: 1
      podRetention:
        policy: Retain
      destination:
        persistentVolumeClaim:
          claimName: %[7]s
    migrate:
      maxRetries: 1
      timeout: 2m
      command: %[8]s
    init:
      command: ["/bin/sh", "-c", "true"]
`, crName, namespace, dbName, mysqlRepo, mysqlTag,
			restrictedLifecyclePodTemplateYAML(), pvcName, migrateCommand("v2")))
	})

	It("dumps MySQL with the default image before each migrate", func() {
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", backupTimeout)
		expectJSONPath("superset", crName, "{.status.lifecycle.backup.state}", "Complete", time.Minute)
		expectJSONPath("job", crName+"-backup", "{.spec.template.spec.containers[0].image}", "mysql:8.4", time.Minute)
		Expect(mysqlExec("SELECT v FROM marker")).To(Equal("v2"))
		files, insert := dumps()
		Expect(files).To(HaveLen(1))
		Expect(insert).To(ContainSubstring("'v1'"))

		patchSuperset(crName, "merge", fmt.Sprintf(`{"spec":{"lifecycle":{"migrate":{"command":%s}}}}`, migrateCommand("v3")))
		Eventually(func(g Gomega) {
			g.Expect(mysqlExec("SELECT v FROM marker")).To(Equal("v3"))
		}, backupTimeout, 2*time.Second).Should(Succeed())
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", backupTimeout)
		files, insert = dumps()
		Expect(files).To(HaveLen(2))
		Expect(insert).To(ContainSubstring("'v2'"))
	})
})

var _ = Describe("Superset lifecycle backup (first install without a database)", func() {
	It("skips the dump when the metastore database does not exist yet", func() {
		const crName = "test-backup-nodb"
		const pgName = crName + "-db"
		const pvcName = crName + "-backups"
		DeferCleanup(func() {
			deleteSuperset(crName)
			_, _ = runKubectl("delete", "deploy,svc,pvc", "-n", namespace, pgName, pvcName, "--ignore-not-found")
			_, _ = runKubectl("delete", "secret", pgName, crName+"-app", "-n", namespace, "--ignore-not-found")
		})

		// POSTGRES_DB=postgres: the "superset" database is only created later by
		// the migrate Job's createDatabase init container.
		applyYAML(pgName, fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %[1]s
  namespace: %[2]s
stringData:
  password: superset-e2e
---
apiVersion: v1
kind: Secret
metadata:
  name: %[4]s-app
  namespace: %[2]s
stringData:
  secret-key: e2e-secret-key-not-for-production
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %[5]s
  namespace: %[2]s
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  selector:
    app: %[1]s
  ports:
  - port: 5432
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %[1]s
  template:
    metadata:
      labels:
        app: %[1]s
    spec:
%[6]s      containers:
      - name: postgres
        image: %[3]s
        env:
        - name: POSTGRES_USER
          value: superset
        - name: POSTGRES_DB
          value: postgres
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: %[1]s
              key: password
        - name: PGDATA
          value: /var/lib/postgresql/data/pgdata
        readinessProbe:
          exec:
            command: ["pg_isready", "-U", "superset", "-h", "127.0.0.1"]
          periodSeconds: 2
%[7]s        volumeMounts:
        - name: data
          mountPath: /var/lib/postgresql/data
        - name: run
          mountPath: /var/run/postgresql
      volumes:
      - name: data
        emptyDir: {}
      - name: run
        emptyDir: {}
`, pgName, namespace, postgresImage, crName, pvcName,
			restrictedSecurityYAML("      ", 70), restrictedContainerSecurityYAML))
		_, err := runKubectl("rollout", "status", "deploy/"+pgName, "-n", namespace, "--timeout=5m")
		Expect(err).NotTo(HaveOccurred())

		pgRepo, pgTag := splitImageRef(postgresImage)
		applyYAML(crName, fmt.Sprintf(`apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  image:
    repository: apache/superset
    tag: "6.1.0"
  environment: Production
  secretKeyFrom:
    name: %[1]s-app
    key: secret-key
  metastore:
    host: %[3]s
    database: superset
    username: superset
    passwordFrom:
      name: %[3]s
      key: password
    createDatabase: true
  lifecycle:
    image:
      repository: %[4]s
      tag: "%[5]s"
%[6]s    podRetention:
      policy: Retain
    backup:
      maxRetries: 1
      destination:
        persistentVolumeClaim:
          claimName: %[7]s
    migrate:
      maxRetries: 1
      command: ["/bin/sh", "-c", "true"]
    init:
      disabled: true
`, crName, namespace, pgName, pgRepo, pgTag, restrictedLifecyclePodTemplateYAML(), pvcName))

		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", backupTimeout)
		expectJSONPath("superset", crName, "{.status.lifecycle.backup.state}", "Complete", time.Minute)
		logs, err := runKubectl("logs", "job/"+crName+"-backup", "-n", namespace)
		Expect(err).NotTo(HaveOccurred())
		Expect(logs).To(ContainSubstring("does not exist yet; nothing to back up"))
		out, err := runKubectl("exec", "-n", namespace, "deploy/"+pgName, "--",
			"psql", "-X", "-U", "superset", "-d", "postgres", "-tA", "-c",
			"SELECT 1 FROM pg_database WHERE datname = 'superset'")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("1"), "createDatabase ran after the skipped backup")
	})
})

var _ = Describe("Superset lifecycle backup (MySQL first install without a database)", func() {
	It("skips the dump, then createDatabase creates the database", func() {
		const crName = "test-backup-mysql-nodb"
		const dbName = crName + "-db"
		const pvcName = crName + "-backups"
		mysqlUID := 999
		DeferCleanup(func() {
			deleteSuperset(crName)
			_, _ = runKubectl("delete", "deploy,svc,pvc", "-n", namespace, dbName, pvcName, "--ignore-not-found")
			_, _ = runKubectl("delete", "secret", dbName, crName+"-app", "-n", namespace, "--ignore-not-found")
		})

		// No MYSQL_DATABASE: only the root account exists, and the "superset"
		// database is created later by the migrate Job's init container.
		applyYAML(dbName, fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %[1]s
  namespace: %[2]s
stringData:
  root-password: root-e2e
---
apiVersion: v1
kind: Secret
metadata:
  name: %[4]s-app
  namespace: %[2]s
stringData:
  secret-key: e2e-secret-key-not-for-production
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %[5]s
  namespace: %[2]s
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  selector:
    app: %[1]s
  ports:
  - port: 3306
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %[1]s
  template:
    metadata:
      labels:
        app: %[1]s
    spec:
%[6]s      containers:
      - name: mysql
        image: %[3]s
        env:
        - name: MYSQL_ROOT_PASSWORD
          valueFrom:
            secretKeyRef:
              name: %[1]s
              key: root-password
        readinessProbe:
          exec:
            command: ["mysql", "-h", "127.0.0.1", "-uroot", "-proot-e2e", "-e", "SELECT 1"]
          periodSeconds: 3
%[7]s        volumeMounts:
        - name: data
          mountPath: /var/lib/mysql
        - name: run
          mountPath: /var/run/mysqld
      volumes:
      - name: data
        emptyDir: {}
      - name: run
        emptyDir: {}
`, dbName, namespace, mysqlImage, crName, pvcName,
			restrictedSecurityYAML("      ", mysqlUID), restrictedContainerSecurityYAML))
		_, err := runKubectl("rollout", "status", "deploy/"+dbName, "-n", namespace, "--timeout=5m")
		Expect(err).NotTo(HaveOccurred())

		mysqlRepo, mysqlTag := splitImageRef(mysqlImage)
		applyYAML(crName, fmt.Sprintf(`apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  image:
    repository: apache/superset
    tag: "6.1.0"
  environment: Production
  secretKeyFrom:
    name: %[1]s-app
    key: secret-key
  metastore:
    type: MySQL
    host: %[3]s
    database: superset
    username: root
    passwordFrom:
      name: %[3]s
      key: root-password
    createDatabase: true
  lifecycle:
    image:
      repository: %[4]s
      tag: "%[5]s"
%[6]s    podRetention:
      policy: Retain
    backup:
      maxRetries: 1
      destination:
        persistentVolumeClaim:
          claimName: %[7]s
    migrate:
      maxRetries: 1
      command: ["/bin/sh", "-c", "true"]
    init:
      disabled: true
`, crName, namespace, dbName, mysqlRepo, mysqlTag, restrictedLifecyclePodTemplateYAML(), pvcName))

		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", backupTimeout)
		expectJSONPath("superset", crName, "{.status.lifecycle.backup.state}", "Complete", time.Minute)
		logs, err := runKubectl("logs", "job/"+crName+"-backup", "-n", namespace)
		Expect(err).NotTo(HaveOccurred())
		Expect(logs).To(ContainSubstring("does not exist yet; nothing to back up"))
		expectJSONPath("job", crName+"-migrate",
			"{.spec.template.spec.initContainers[?(@.name=='create-database')].image}", "mysql:8.4", time.Minute)
		out, err := runKubectl("exec", "-n", namespace, "deploy/"+dbName, "--",
			"mysql", "-uroot", "-proot-e2e", "-N", "-B", "-e", "SHOW DATABASES LIKE 'superset'")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(ContainSubstring("superset"), "createDatabase ran after the skipped backup")
	})
})

var _ = Describe("Superset lifecycle backup (restore runbook)", Ordered, func() {
	const crName = "test-backup-restore"
	const pgName = crName + "-db"
	const pvcName = crName + "-backups"
	pgUID := 70

	psql := func(sql string) string {
		out, err := runKubectl("exec", "-n", namespace, "deploy/"+pgName, "--",
			"psql", "-X", "-U", "superset", "-d", "superset", "-tA", "-v", "ON_ERROR_STOP=1", "-c", sql)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		return strings.TrimSpace(out)
	}
	migrateCommand := func(value string) string {
		return fmt.Sprintf(`["/bin/sh", "-c", "PGPASSWORD=\"$SUPERSET_OPERATOR__DB_PASS\" psql -X -v ON_ERROR_STOP=1 `+
			`-h \"$SUPERSET_OPERATOR__DB_HOST\" -U \"$SUPERSET_OPERATOR__DB_USER\" -d \"$SUPERSET_OPERATOR__DB_NAME\" `+
			`-c \"UPDATE marker SET v = '%s'\""]`, value)
	}
	dumpCount := func() string {
		logs := runVerifierPod(crName+"-count", postgresImage, pvcName, pgUID, `ls -1 /backup/*.dump | wc -l`)
		return strings.TrimSpace(logs)
	}
	newest := func() string {
		logs := runVerifierPod(crName+"-newest", postgresImage, pvcName, pgUID, `ls -1 /backup/*.dump | sort | tail -n 1`)
		return strings.TrimSpace(logs)
	}

	BeforeAll(func() {
		DeferCleanup(func() {
			deleteSuperset(crName)
			_, _ = runKubectl("delete", "deploy,svc,pvc", "-n", namespace, pgName, pvcName, "--ignore-not-found")
			_, _ = runKubectl("delete", "secret", pgName, crName+"-app", "-n", namespace, "--ignore-not-found")
		})

		applyYAML(pgName, fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %[1]s
  namespace: %[2]s
stringData:
  password: superset-e2e
---
apiVersion: v1
kind: Secret
metadata:
  name: %[4]s-app
  namespace: %[2]s
stringData:
  secret-key: e2e-secret-key-not-for-production
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %[5]s
  namespace: %[2]s
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  selector:
    app: %[1]s
  ports:
  - port: 5432
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %[1]s
  template:
    metadata:
      labels:
        app: %[1]s
    spec:
%[6]s      containers:
      - name: postgres
        image: %[3]s
        env:
        - name: POSTGRES_USER
          value: superset
        - name: POSTGRES_DB
          value: superset
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: %[1]s
              key: password
        - name: PGDATA
          value: /var/lib/postgresql/data/pgdata
        readinessProbe:
          exec:
            command: ["pg_isready", "-U", "superset", "-d", "superset", "-h", "127.0.0.1"]
          periodSeconds: 2
%[7]s        volumeMounts:
        - name: data
          mountPath: /var/lib/postgresql/data
        - name: run
          mountPath: /var/run/postgresql
      volumes:
      - name: data
        emptyDir: {}
      - name: run
        emptyDir: {}
`, pgName, namespace, postgresImage, crName, pvcName,
			restrictedSecurityYAML("      ", pgUID), restrictedContainerSecurityYAML))
		_, err := runKubectl("rollout", "status", "deploy/"+pgName, "-n", namespace, "--timeout=5m")
		Expect(err).NotTo(HaveOccurred())
		psql("CREATE TABLE marker (v text); INSERT INTO marker VALUES ('v1')")

		pgRepo, pgTag := splitImageRef(postgresImage)
		applyYAML(crName, fmt.Sprintf(`apiVersion: superset.apache.org/v1alpha1
kind: Superset
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  image:
    repository: apache/superset
    tag: "6.1.0"
  environment: Production
  secretKeyFrom:
    name: %[1]s-app
    key: secret-key
  metastore:
    host: %[3]s
    database: superset
    username: superset
    passwordFrom:
      name: %[3]s
      key: password
  lifecycle:
    image:
      repository: %[4]s
      tag: "%[5]s"
%[6]s    backup:
      maxRetries: 1
      destination:
        persistentVolumeClaim:
          claimName: %[7]s
    migrate:
      maxRetries: 1
      timeout: 2m
      command: %[8]s
    init:
      command: ["/bin/sh", "-c", "true"]
`, crName, namespace, pgName, pgRepo, pgTag, restrictedLifecyclePodTemplateYAML(), pvcName, migrateCommand("v2")))
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", backupTimeout)
		Expect(psql("SELECT v FROM marker")).To(Equal("v2"))
	})

	It("takes a pre-upgrade snapshot before an unwanted upgrade", func() {
		patchSuperset(crName, "merge", fmt.Sprintf(`{"spec":{"lifecycle":{"migrate":{"command":%s}}}}`, migrateCommand("v3")))
		Eventually(func(g Gomega) {
			g.Expect(psql("SELECT v FROM marker")).To(Equal("v3"))
		}, backupTimeout, 2*time.Second).Should(Succeed())
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", backupTimeout)
		Expect(dumpCount()).To(Equal("2"))
	})

	It("keeps dumps unreadable to other UIDs", func() {
		dump := newest()
		logs := runVerifierPod(crName+"-other-uid", postgresImage, pvcName, 1000,
			fmt.Sprintf(`if cat %q >/dev/null 2>&1; then echo READABLE; else echo DENIED; fi`, dump))
		Expect(strings.TrimSpace(logs)).To(Equal("DENIED"))
	})

	It("restores the snapshot with the runbook and resumes reconciliation", func() {
		By("suspending reconciliation")
		patchSuperset(crName, "merge", `{"spec":{"suspend":true}}`)
		expectJSONPath("superset", crName, "{.status.phase}", "Suspended", time.Minute)
		backupBefore, err := jsonPath("superset", crName, "{.status.lifecycle.backup.completedChecksum}")
		Expect(err).NotTo(HaveOccurred())

		By("restoring the newest dump as the backup UID")
		dump := newest()
		logs := runPVCPod(crName+"-restore", postgresImage, pvcName, pgUID, fmt.Sprintf(`    - name: PGPASSWORD
      valueFrom:
        secretKeyRef:
          name: %s
          key: password
`, pgName), fmt.Sprintf(`set -eu
pg_restore --clean --if-exists --no-owner -h %s -U superset -d superset %q
echo RESTORED`, pgName, dump))
		Expect(logs).To(ContainSubstring("RESTORED"))
		Expect(psql("SELECT v FROM marker")).To(Equal("v2"), "the database is back at the pre-upgrade state")

		By("reverting the upgrade and resuming")
		patchSuperset(crName, "merge", fmt.Sprintf(
			`{"spec":{"suspend":false,"lifecycle":{"migrate":{"command":%s}}}}`, migrateCommand("v2")))
		expectJSONPath("superset", crName, "{.status.lifecycle.phase}", "Complete", backupTimeout)
		Eventually(func(g Gomega) {
			after, err := jsonPath("superset", crName, "{.status.lifecycle.backup.completedChecksum}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(after).NotTo(Equal(backupBefore), "the resumed run snapshots the restored database first")
		}, backupTimeout, 2*time.Second).Should(Succeed())
		Expect(psql("SELECT v FROM marker")).To(Equal("v2"))
		Expect(dumpCount()).To(Equal("3"))
	})
})
