package dev.hot.sdk;

import dev.hot.sdk.internal.Transport;

/**
 * Projects, builds and context of one environment, reached with an org API key through {@code
 * /v1/orgs/{org_id}/envs/{env_id}}. The resources behave like {@link HotClient#projects()},
 * {@link HotClient#builds()} and {@link HotClient#context()} for an environment key holding the
 * org key's grants for that environment. Get one from {@link OrgsResource#env}.
 */
public final class OrgEnv {

  private final ProjectsResource projects;
  private final BuildsResource builds;
  private final ContextResource context;

  OrgEnv(Transport transport, String prefix) {
    this.projects = new ProjectsResource(transport, prefix);
    this.builds = new BuildsResource(transport, prefix);
    this.context = new ContextResource(transport, prefix);
  }

  /** Projects, including activation, event handlers and schedules. */
  public ProjectsResource projects() {
    return projects;
  }

  /** Builds: upload, list, download and deploy. */
  public BuildsResource builds() {
    return builds;
  }

  /** Project context variables. */
  public ContextResource context() {
    return context;
  }
}
