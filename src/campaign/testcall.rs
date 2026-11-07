//! One call placed outside any campaign, to try outbound calling before the campaign
//! dialer runs (`OUTBOUND_TEST_CALL`).

use super::runner::CallPlacer;
use crate::{
    ari::outbound::DialRequest,
    dograh::{
        Store,
        campaign::{Campaign, QueuedRun},
        outbound::STATUS_FAILED,
    },
};
use anyhow::{Result, bail};
use std::{sync::Arc, time::Duration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestCall {
    pub number: String,
    pub org_id: i32,
    pub workflow_id: i32,
}

/// Reads "<number>,<org id>,<workflow id>".
pub fn parse(spec: &str) -> Result<TestCall> {
    let parts: Vec<&str> = spec.split(',').map(str::trim).collect();
    if let [number, org, wf] = parts[..]
        && !number.is_empty()
        && let (Ok(org_id), Ok(workflow_id)) = (org.parse::<i32>(), wf.parse::<i32>())
        && org_id > 0
        && workflow_id > 0
    {
        return Ok(TestCall { number: number.to_string(), org_id, workflow_id });
    }
    bail!(r#"OUTBOUND_TEST_CALL must be "<number>,<org id>,<workflow id>", got {spec:?}"#)
}

/// Dials `tc` the way a campaign call is dialed: the organization's dial settings, a Dograh run created
/// first, the agent on answer, and a call that never connects completed in Dograh with its status. Returns
/// once the call is placed; the outcome is logged when it arrives.
pub async fn run(store: Store, placer: Arc<dyn CallPlacer>, tc: TestCall, ring: Duration) -> Result<()> {
    let settings = store.dial_settings(tc.org_id).await?;
    if settings.provider != "ari" {
        bail!("organization {}'s telephony provider is {:?}, not ari", tc.org_id, settings.provider);
    }
    let caller = settings.caller_id(tc.workflow_id);
    let campaign = Campaign { organization_id: tc.org_id, workflow_id: tc.workflow_id, ..Default::default() };
    let run = store.create_outbound_run(&campaign, &QueuedRun::default(), &tc.number, &caller).await?;

    let (st, org) = (store.clone(), tc.org_id);
    let req = DialRequest {
        endpoint: settings.endpoint(&tc.number),
        caller_id: caller,
        called: tc.number.clone(),
        ring_timeout: ring,
        workflow_id: tc.workflow_id,
        run_id: run,
        variables: Default::default(),
        done: Box::new(move |out| {
            tokio::spawn(async move {
                if out.connected {
                    return tracing::info!(event = "outbound.test_done", run_id = run, connected = true, "outbound test call finished");
                }
                tracing::info!(
                    event = "outbound.test_done",
                    run_id = run,
                    connected = false,
                    status = out.status,
                    cause = out.cause,
                    "outbound test call did not connect"
                );
                if let Err(e) = st.mark_not_connected(run, org, out.status).await {
                    tracing::error!(run_id = run, error = %format!("{e:#}"), "outbound test call: completing its run failed");
                }
            });
        }),
    };
    if let Err(e) = placer.dial(req).await {
        // Nothing was dialed: complete the run so it doesn't stay 'initialized'.
        let _ = store.mark_not_connected(run, tc.org_id, STATUS_FAILED).await;
        return Err(e);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_spec_is_number_org_workflow() {
        assert_eq!(parse(" +919876543210 , 3, 19").unwrap(), TestCall { number: "+919876543210".into(), org_id: 3, workflow_id: 19 });
        for bad in ["", "123", "1,2", "1,2,3,4", ",3,4", "1,x,4", "1,0,4", "1,3,-1"] {
            assert!(parse(bad).is_err(), "{bad:?}");
        }
    }
}
