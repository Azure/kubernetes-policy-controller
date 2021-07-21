#!/bin/bash
source test/bats/helpers.bash

WAIT_TIME=120
SLEEP_TIME=1

wait_for_process ${WAIT_TIME} ${SLEEP_TIME} "kubectl wait -n gatekeeper-system --for=condition=Ready --timeout=60s pod -l control-plane=audit-controller"
wait_for_process ${WAIT_TIME} ${SLEEP_TIME} "kubectl wait -n gatekeeper-system --for=condition=Ready --timeout=60s pod -l control-plane=controller-manager"
wait_for_process ${WAIT_TIME} ${SLEEP_TIME} "kubectl wait --for condition=established --timeout=60s crd/constrainttemplates.templates.gatekeeper.sh"
wait_for_process ${WAIT_TIME} ${SLEEP_TIME} "kubectl wait --for condition=established --timeout=60s crd/configs.config.gatekeeper.sh"

# deploying templates
t="1"
while [ $t -le $NUMBER_TEMPLATES ]; do
    export TEMPLATE_NAME=template-$(openssl rand -hex 6)

    envsubst <test/load/allowedrepos-ct-template.yaml >test/load/allowedrepos-ct.yaml
    kubectl apply -f test/load/allowedrepos-ct.yaml

    wait_for_process ${WAIT_TIME} ${SLEEP_TIME} "kubectl wait --for condition=established --timeout=60s crd/$TEMPLATE_NAME.constraints.gatekeeper.sh"
    t=$(($t + 1))

    # number of constraints per template
    c="1"
    while [ $c -le $NUMBER_CONSTRAINTS ]; do
        export CONSTRAINT_NAME=repo-$(openssl rand -hex 6)

        envsubst <test/load/allowedrepos-constraint-template.yaml >test/load/allowedrepos-constraint.yaml
        kubectl apply -f test/load/allowedrepos-constraint.yaml

        wait_for_process ${WAIT_TIME} ${SLEEP_TIME} "constraint_enforced $TEMPLATE_NAME $CONSTRAINT_NAME"
        c=$(($c + 1))
    done
done
