use leptos::mount::mount_to_body;
use leptos::prelude::*;

const SOURCE_REVISION: &str = env!("MYKO_SOURCE_REVISION");
const SOURCE_BRANCH: &str = env!("MYKO_SOURCE_BRANCH");
const SOURCE_STATE: &str = env!("MYKO_SOURCE_STATE");
const REPOSITORY: &str = "https://github.com/ignition-is-go/myko";

#[derive(Clone, Copy, PartialEq, Eq)]
enum FlowKind {
    Command,
    State,
    Replication,
}

#[derive(Clone, Copy)]
struct FlowStep {
    number: &'static str,
    title: &'static str,
    owner: &'static str,
    detail: &'static str,
}

const COMMAND_STEPS: &[FlowStep] = &[
    FlowStep {
        number: "01",
        title: "Declare intent",
        owner: "application",
        detail: "A typed command body names its service, output, and optional item boundary at compile time.",
    },
    FlowStep {
        number: "02",
        title: "Address the work",
        owner: "client edge",
        detail: "The app supplies a stable command ID, target scope, and authenticated principal context.",
    },
    FlowStep {
        number: "03",
        title: "Persist admission",
        owner: "authoritative node",
        detail: "Submission enters immutable history before execution. Conflicting reuse of an ID is rejected.",
    },
    FlowStep {
        number: "04",
        title: "Run once locally",
        owner: "application node",
        detail: "Only the origin node claims executable work. Replicated submissions remain observable projections.",
    },
    FlowStep {
        number: "05",
        title: "Commit one batch",
        owner: "command context",
        detail: "Typed mutations and the result commit atomically, or the command is rejected, retried, or cancelled.",
    },
    FlowStep {
        number: "06",
        title: "Wake the graph",
        owner: "reactive runtime",
        detail: "Item projections update at one log position; dependent queries, reports, views, and UI cells react.",
    },
];

const STATE_STEPS: &[FlowStep] = &[
    FlowStep {
        number: "01",
        title: "Replay facts",
        owner: "event journal",
        detail: "Accepted command lifecycle events and atomic mutation batches are the durable source of truth.",
    },
    FlowStep {
        number: "02",
        title: "Materialize items",
        owner: "federation node",
        detail: "The node reconstructs current typed items by source, service, scope, type, and stable ID.",
    },
    FlowStep {
        number: "03",
        title: "Run typed queries",
        owner: "myko-items",
        detail: "ItemQuery receives a typed projection, never raw envelopes or transport cursors.",
    },
    FlowStep {
        number: "04",
        title: "Compose dependencies",
        owner: "myko-app + Hyphae",
        detail: "Registered queries feed application reports and views through a retained reactive graph.",
    },
    FlowStep {
        number: "05",
        title: "Snapshot, then follow",
        owner: "session boundary",
        detail: "A fixed log ceiling and a follow cursor form one gap-free current-then-live stream.",
    },
    FlowStep {
        number: "06",
        title: "Render coherent state",
        owner: "UI adapter",
        detail: "Leptos, Ratatui, or another adapter bridges the same cell; it does not create a second state store.",
    },
];

const REPLICATION_STEPS: &[FlowStep] = &[
    FlowStep {
        number: "01",
        title: "Verify identity",
        owner: "Iroh endpoint",
        detail: "A native descriptor binds the authenticated Iroh endpoint to the expected Myko source log.",
    },
    FlowStep {
        number: "02",
        title: "Resume a source cursor",
        owner: "peer supervisor",
        detail: "Each followed source retains an independent durable checkpoint across reconnects and restarts.",
    },
    FlowStep {
        number: "03",
        title: "Pull immutable batches",
        owner: "myko-iroh",
        detail: "Followers request complete history or a scope-filtered subset strictly after their checkpoint.",
    },
    FlowStep {
        number: "04",
        title: "Ingest without executing",
        owner: "replica node",
        detail: "Remote facts preserve their origin. A replica materializes them but never claims foreign commands.",
    },
    FlowStep {
        number: "05",
        title: "Project remote state",
        owner: "typed state layer",
        detail: "Queries can select one authoritative source or compose state across sources explicitly.",
    },
    FlowStep {
        number: "06",
        title: "Advance after ingest",
        owner: "cursor store",
        detail: "The checkpoint moves only after a valid batch is accepted, preventing acknowledged history loss.",
    },
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum BuildTab {
    Model,
    Command,
    Handler,
    Compose,
}

#[derive(Clone, Copy)]
struct BuildExample {
    eyebrow: &'static str,
    title: &'static str,
    detail: &'static str,
    source: &'static str,
    code: &'static str,
}

const MODEL_EXAMPLE: BuildExample = BuildExample {
    eyebrow: "1 / model",
    title: "Choose the atomicity boundary",
    detail: "The service catalog and item live in different Forrest modules. Together they declare the atomicity boundary and the item's scope.",
    source: "forrest-core/src/lib.rs + mailbox.rs",
    code: r"// forrest-core/src/lib.rs
#[myko_service(mailbox::AgentMessage)]
pub struct MessagingService;

// forrest-core/src/mailbox.rs
#[myko_item(
    service = crate::MessagingService,
    scoped_by = Agent
)]
pub struct AgentMessage {
    pub from: AgentId,
    pub to: AgentId,
    pub reply_to: Option<AgentMessageId>,
    pub blocks: Vec<ContentBlock>,
}",
};

const COMMAND_EXAMPLE: BuildExample = BuildExample {
    eyebrow: "2 / intent",
    title: "Declare a bounded operation",
    detail: "The macro generates the stable service and command identities, serialization contract, and typed output association.",
    source: "forrest-core/src/mailbox.rs",
    code: r"#[myko_command(DeliveryStatus, item = AgentMessage)]
pub struct SendAgentMessage {
    pub message: AgentMessage,
}",
};

const HANDLER_EXAMPLE: BuildExample = BuildExample {
    eyebrow: "3 / execute",
    title: "Enforce invariants, emit typed state",
    detail: "The handler sees a scoped capability. It cannot silently write an item owned by another service or commit outside the command scope.",
    source: "forrest-core/src/mailbox.rs",
    code: r"impl CommandHandler for SendAgentMessage {
    fn scope(&self, _node_id: NodeId) -> AgentId {
        self.message.to.clone()
    }

    fn execute(
        self,
        context: CommandContext<MessagingService, Agent>,
    ) -> Result<Self::Output, CommandError> {
        validate_message_envelope(
            &self.message,
            context.scope_id(),
            context.principal_id(),
        )
        .map_err(CommandError::reject)?;

        context.emit_set(&self.message)?;
        Ok(DeliveryStatus::Delivered)
    }
}",
};

const COMPOSE_EXAMPLE: BuildExample = BuildExample {
    eyebrow: "4 / compose",
    title: "Activate services once",
    detail: "Forrest explicitly activates its service catalog, then attaches that immutable application declaration to whichever Myko node substrate it opens.",
    source: "forrest-node/src/application.rs",
    code: r"pub fn myko_application() -> Result<MykoApplication, AppError> {
    let builder = MykoApplication::builder()
        .service::<HostingService>()?
        .service::<AgentService>()?
        .service::<MessagingService>()?
        .service::<AccessService>()?;
    Ok(builder.build())
}

pub fn application_node(node: Node) -> Result<ApplicationNode, AppError> {
    Ok(ApplicationNode::new(node, myko_application()?))
}",
};

#[derive(Clone, Copy)]
struct CrateInfo {
    name: &'static str,
    layer: &'static str,
    summary: &'static str,
    path: &'static str,
}

const CRATES: &[CrateInfo] = &[
    CrateInfo {
        name: "myko-items",
        layer: "contract",
        summary: "Typed service, item, mutation, projection, and query contracts.",
        path: "libs/myko/items",
    },
    CrateInfo {
        name: "myko-items-macros",
        layer: "contract",
        summary: "Generates stable IDs and schemas for services, items, commands, and subtypes.",
        path: "libs/myko/items-macros",
    },
    CrateInfo {
        name: "myko-app",
        layer: "application",
        summary: "Service catalog, bounded handlers, resources, reactive queries, reports, and views.",
        path: "libs/myko/app",
    },
    CrateInfo {
        name: "myko-app-macros",
        layer: "application",
        summary: "Scoped handler registration generated beside the application contracts that own it.",
        path: "libs/myko/app-macros",
    },
    CrateInfo {
        name: "myko-federation",
        layer: "semantic core",
        summary: "Transport-neutral nodes, command lifecycles, history, scopes, projections, and follows.",
        path: "libs/myko/federation",
    },
    CrateInfo {
        name: "myko-redb",
        layer: "durability",
        summary: "Crash-safe immutable event journal and durable replication checkpoints.",
        path: "libs/myko/redb",
    },
    CrateInfo {
        name: "myko-wire",
        layer: "boundary",
        summary: "Canonical, bounded, versioned request and frame envelopes.",
        path: "libs/myko/wire",
    },
    CrateInfo {
        name: "myko-session",
        layer: "boundary",
        summary: "Transport-neutral authorization, request routing, snapshots, and live stream lifecycle.",
        path: "libs/myko/session",
    },
    CrateInfo {
        name: "myko-iroh",
        layer: "native network",
        summary: "Authenticated commands, typed streams, pairing, replication, and peer following over Iroh.",
        path: "libs/myko/iroh",
    },
    CrateInfo {
        name: "myko-local",
        layer: "local edge",
        summary: "Owner-local Unix transport for apps that share a machine with a node.",
        path: "libs/myko/local",
    },
    CrateInfo {
        name: "myko-node",
        layer: "composition",
        summary: "Restartable Redb + Iroh runtime with stable identities and supervised peers.",
        path: "libs/myko/node",
    },
    CrateInfo {
        name: "myko-discovery",
        layer: "bootstrap",
        summary: "Bounded local-network discovery for native node descriptors.",
        path: "libs/myko/discovery",
    },
    CrateInfo {
        name: "myko-ratatui",
        layer: "UI lifecycle",
        summary: "Retains reactive cells and coalesces redraw wakeups without becoming a widget library.",
        path: "libs/myko/ratatui",
    },
    CrateInfo {
        name: "myko-websocket-gateway",
        layer: "optional edge",
        summary: "Compatibility gateway for short-lived browser and WebSocket clients.",
        path: "libs/myko/websocket-gateway",
    },
];

const fn flow_steps(kind: FlowKind) -> &'static [FlowStep] {
    match kind {
        FlowKind::Command => COMMAND_STEPS,
        FlowKind::State => STATE_STEPS,
        FlowKind::Replication => REPLICATION_STEPS,
    }
}

const fn flow_note(kind: FlowKind) -> (&'static str, &'static str) {
    match kind {
        FlowKind::Command => (
            "The authority rule",
            "The node that admitted a command is the only node allowed to execute it. Replication distributes the outcome, not duplicate work.",
        ),
        FlowKind::State => (
            "The coherence rule",
            "A snapshot and its follow stream share one boundary. Consumers never patch a race between a point-in-time query and a later subscription.",
        ),
        FlowKind::Replication => (
            "The source rule",
            "Every replicated fact keeps its authoritative source identity. A serving node and a source node are different dimensions in the state model.",
        ),
    }
}

const fn build_example(tab: BuildTab) -> BuildExample {
    match tab {
        BuildTab::Model => MODEL_EXAMPLE,
        BuildTab::Command => COMMAND_EXAMPLE,
        BuildTab::Handler => HANDLER_EXAMPLE,
        BuildTab::Compose => COMPOSE_EXAMPLE,
    }
}

fn source_url(path: &str) -> String {
    let extension = std::path::Path::new(path)
        .extension()
        .and_then(std::ffi::OsStr::to_str);
    let is_file = extension.is_some_and(|extension| {
        extension.eq_ignore_ascii_case("rs") || extension.eq_ignore_ascii_case("md")
    });
    let view = if is_file { "blob" } else { "tree" };
    format!("{REPOSITORY}/{view}/{SOURCE_REVISION}/{path}")
}

#[component]
fn Mark() -> impl IntoView {
    view! {
        <span class="mark" aria-hidden="true">
            <span class="mark-orbit"></span>
            <span class="mark-core"></span>
        </span>
    }
}

#[component]
fn ArrowIcon() -> impl IntoView {
    view! {
        <svg viewBox="0 0 18 18" aria-hidden="true">
            <path d="M3 9h11M10 5l4 4-4 4"></path>
        </svg>
    }
}

#[component]
fn MyceliumField() -> impl IntoView {
    view! {
        <svg class="mycelium-field" viewBox="0 0 900 700" aria-hidden="true">
            <g class="hyphae hyphae-primary">
                <path d="M-20 602C98 554 121 470 219 443S365 482 418 356 548 156 685 215 810 262 924 66"></path>
                <path d="M219 443C185 354 119 321 48 327"></path>
                <path d="M219 443C282 374 301 278 275 201"></path>
                <path d="M418 356C493 364 536 433 648 455S805 439 919 518"></path>
                <path d="M418 356C392 260 420 194 474 125S521 41 534-29"></path>
                <path d="M685 215C647 142 652 74 706 15"></path>
                <path d="M685 215C754 180 818 182 901 215"></path>
            </g>
            <g class="hyphae hyphae-secondary">
                <path d="M-18 655C118 639 169 568 262 574S405 650 493 560 614 433 747 514 851 612 925 606"></path>
                <path d="M262 574C317 516 327 442 315 383"></path>
                <path d="M493 560C536 615 544 665 539 720"></path>
                <path d="M747 514C758 441 804 382 893 353"></path>
                <path d="M48 327C51 235 97 168 181 124S269 46 263-18"></path>
                <path d="M275 201C337 166 377 113 387 45"></path>
            </g>
            <g class="spores">
                <circle cx="48" cy="327" r="4"></circle>
                <circle cx="181" cy="124" r="3"></circle>
                <circle cx="219" cy="443" r="6"></circle>
                <circle cx="275" cy="201" r="4"></circle>
                <circle cx="315" cy="383" r="3"></circle>
                <circle cx="418" cy="356" r="7"></circle>
                <circle cx="474" cy="125" r="4"></circle>
                <circle cx="493" cy="560" r="5"></circle>
                <circle cx="648" cy="455" r="4"></circle>
                <circle cx="685" cy="215" r="6"></circle>
                <circle cx="747" cy="514" r="4"></circle>
                <circle cx="893" cy="353" r="3"></circle>
            </g>
        </svg>
    }
}

#[component]
fn SourceBadge(path: &'static str) -> impl IntoView {
    let href = source_url(path);
    view! {
        <a class="source-badge" href=href target="_blank" rel="noreferrer">
            <span class="source-dot"></span>
            {path}
        </a>
    }
}

#[component]
fn App() -> impl IntoView {
    let (menu_open, set_menu_open) = signal(false);
    let (active_flow, set_active_flow) = signal(FlowKind::Command);
    let (active_build_tab, set_active_build_tab) = signal(BuildTab::Model);

    view! {
        <div class="page-shell">
            <header class="topbar">
                <a class="brand" href="#top" on:click=move |_| set_menu_open.set(false)>
                    <Mark />
                    <span class="brand-word">"myko"</span>
                    <span class="brand-version">"07"</span>
                </a>

                <button
                    class="menu-toggle"
                    class:is-open=move || menu_open.get()
                    aria-label="Toggle navigation"
                    aria-expanded=move || if menu_open.get() { "true" } else { "false" }
                    on:click=move |_| set_menu_open.update(|open| *open = !*open)
                >
                    <span></span><span></span>
                </button>

                <nav class="topnav" class:is-open=move || menu_open.get()>
                    <a href="#model" on:click=move |_| set_menu_open.set(false)>"Model"</a>
                    <a href="#runtime" on:click=move |_| set_menu_open.set(false)>"Runtime"</a>
                    <a href="#build" on:click=move |_| set_menu_open.set(false)>"Build"</a>
                    <a href="#federation" on:click=move |_| set_menu_open.set(false)>"Federation"</a>
                    <a href="#crates" on:click=move |_| set_menu_open.set(false)>"Crates"</a>
                    <a class="nav-source" href=REPOSITORY target="_blank" rel="noreferrer">
                        "Source" <ArrowIcon />
                    </a>
                </nav>
            </header>

            <main id="top">
                <section class="hero section-pad">
                    <MyceliumField />

                    <div class="hero-copy reveal">
                        <div class="eyebrow-row">
                            <span class="eyebrow">"Architecture field guide"</span>
                            <span class="source-revision">{format!("{SOURCE_BRANCH} · {SOURCE_REVISION} · {SOURCE_STATE}")}</span>
                        </div>
                        <h1>
                            "State is a log. "
                            <span>"Execution has an owner."</span>
                        </h1>
                        <p class="hero-lede">
                            "Myko is a typed application framework for building durable, reactive, federated systems—without making application code own replay, cursors, replication, or transport mechanics."
                        </p>
                        <div class="hero-actions">
                            <a class="button button-primary" href="#runtime">
                                "Trace the runtime" <ArrowIcon />
                            </a>
                            <a class="button button-ghost" href="#build">"Build an application"</a>
                        </div>
                    </div>

                    <div class="hero-system" role="group" aria-label="Myko system summary">
                        <div class="system-label system-label-top">"APPLICATION INTENT"</div>
                        <div class="system-card system-card-app">
                            <div class="system-card-head">
                                <span class="status-light"></span>
                                <span>"Your application"</span>
                                <span class="system-code">"typed"</span>
                            </div>
                            <div class="mini-code">
                                <span><b>"service"</b> "Messaging"</span>
                                <span><b>"command"</b> "SendAgentMessage"</span>
                                <span><b>"query"</b> "OrderedMailbox"</span>
                            </div>
                        </div>
                        <div class="system-connector">
                            <span></span><em>"bounded context"</em><span></span>
                        </div>
                        <div class="system-card system-card-core">
                            <div class="system-card-head">
                                <span class="status-light status-light-cyan"></span>
                                <span>"Myko runtime"</span>
                                <span class="system-code">"durable"</span>
                            </div>
                            <div class="runtime-pulse">
                                <span>"admit"</span><i></i><span>"execute"</span><i></i><span>"commit"</span>
                            </div>
                        </div>
                        <div class="system-split">
                            <div class="split-line"></div>
                            <div class="split-target"><span>"01"</span>"Redb history"</div>
                            <div class="split-target"><span>"02"</span>"Iroh federation"</div>
                            <div class="split-target"><span>"03"</span>"Hyphae graph"</div>
                        </div>
                    </div>

                    <div class="hero-proof">
                        <span>"TYPED CONTRACTS"</span>
                        <span>"IMMUTABLE HISTORY"</span>
                        <span>"SOURCE-AWARE REPLICATION"</span>
                        <span>"GAP-FREE FOLLOWS"</span>
                    </div>
                </section>

                <section class="thesis section-pad" id="model">
                    <div class="section-intro">
                        <span class="section-number">"01"</span>
                        <div>
                            <p class="kicker">"The working model"</p>
                            <h2>"Three ideas carry the whole architecture."</h2>
                        </div>
                        <p class="section-summary">
                            "Myko keeps domain semantics in the application and pushes distributed-systems mechanics below a narrow typed boundary."
                        </p>
                    </div>

                    <div class="truth-grid">
                        <article class="truth-card truth-card-lime">
                            <div class="truth-index">"A"</div>
                            <div class="truth-graphic atomicity-graphic">
                                <span></span><span></span><span></span>
                                <div>"1 batch"</div>
                            </div>
                            <h3>"Services define atomicity"</h3>
                            <p>"A service is a semantic boundary—not a process or crate. One command may atomically mutate the item modules owned by that service."</p>
                            <div class="truth-foot">"service → items → commands"</div>
                        </article>

                        <article class="truth-card truth-card-orange">
                            <div class="truth-index">"B"</div>
                            <div class="truth-graphic authority-graphic">
                                <div class="authority-origin">"origin"</div>
                                <span></span>
                                <div class="authority-copy">"replica"</div>
                            </div>
                            <h3>"Execution stays authoritative"</h3>
                            <p>"The node that admitted a command owns its execution. Other nodes may replicate and query the lifecycle, but never turn it into duplicate work."</p>
                            <div class="truth-foot">"facts travel · authority does not"</div>
                        </article>

                        <article class="truth-card truth-card-cyan">
                            <div class="truth-index">"C"</div>
                            <div class="truth-graphic projection-graphic">
                                <div class="event-dots"><i></i><i></i><i></i><i></i></div>
                                <span></span>
                                <div class="projection-box">"view"</div>
                            </div>
                            <h3>"Everything current is a projection"</h3>
                            <p>"Durable history is replayed into typed item state. Queries, reports, views, and UI cells are retained projections over that state."</p>
                            <div class="truth-foot">"history → state → reactive graph"</div>
                        </article>
                    </div>

                    <div class="model-stack">
                        <div class="model-stack-copy">
                            <p class="kicker">"Separation of concerns"</p>
                            <h3>"The application describes meaning. Myko carries time and distance."</h3>
                            <p>"Application code owns types, invariants, scopes, handlers, queries, views, and access policy. Myko owns identity, immutable history, current-state materialization, resumable subscriptions, persistence, and replication."</p>
                            <SourceBadge path="README.md" />
                        </div>
                        <div class="layer-stack" role="group" aria-label="Myko architecture layers">
                            <div class="layer-row layer-app">
                                <span class="layer-no">"L0"</span>
                                <div><b>"Application vertical slices"</b><small>"services · items · commands · queries · reports · views"</small></div>
                                <code>"your crates"</code>
                            </div>
                            <div class="layer-row layer-runtime">
                                <span class="layer-no">"L1"</span>
                                <div><b>"Application runtime"</b><small>"catalog · handlers · resources · Hyphae graph"</small></div>
                                <code>"myko-app"</code>
                            </div>
                            <div class="layer-row layer-core">
                                <span class="layer-no">"L2"</span>
                                <div><b>"Federation semantics"</b><small>"command lifecycle · log · scopes · projections · follows"</small></div>
                                <code>"myko-federation"</code>
                            </div>
                            <div class="layer-row layer-session">
                                <span class="layer-no">"L3"</span>
                                <div><b>"Authorized sessions"</b><small>"versioned requests · routing · snapshot/live boundaries"</small></div>
                                <code>"wire + session"</code>
                            </div>
                            <div class="layer-row layer-adapters">
                                <span class="layer-no">"L4"</span>
                                <div><b>"Replaceable mechanisms"</b><small>"Redb · Iroh · Unix socket · WebSocket edge"</small></div>
                                <code>"adapters"</code>
                            </div>
                        </div>
                    </div>
                </section>

                <section class="runtime section-pad section-dark" id="runtime">
                    <div class="section-intro section-intro-light">
                        <span class="section-number">"02"</span>
                        <div>
                            <p class="kicker">"Runtime traces"</p>
                            <h2>"Follow one thing all the way through."</h2>
                        </div>
                        <p class="section-summary">
                            "Switch lenses to see where execution, state, and replication deliberately take different paths."
                        </p>
                    </div>

                    <div class="flow-tabs" role="tablist" aria-label="Runtime flow">
                        <button
                            role="tab"
                            id="runtime-tab-command"
                            aria-controls="runtime-panel"
                            class:active=move || active_flow.get() == FlowKind::Command
                            aria-selected=move || if active_flow.get() == FlowKind::Command { "true" } else { "false" }
                            on:click=move |_| set_active_flow.set(FlowKind::Command)
                        ><span>"01"</span>"Command"</button>
                        <button
                            role="tab"
                            id="runtime-tab-state"
                            aria-controls="runtime-panel"
                            class:active=move || active_flow.get() == FlowKind::State
                            aria-selected=move || if active_flow.get() == FlowKind::State { "true" } else { "false" }
                            on:click=move |_| set_active_flow.set(FlowKind::State)
                        ><span>"02"</span>"Reactive state"</button>
                        <button
                            role="tab"
                            id="runtime-tab-replication"
                            aria-controls="runtime-panel"
                            class:active=move || active_flow.get() == FlowKind::Replication
                            aria-selected=move || if active_flow.get() == FlowKind::Replication { "true" } else { "false" }
                            on:click=move |_| set_active_flow.set(FlowKind::Replication)
                        ><span>"03"</span>"Replication"</button>
                    </div>

                    <div
                        class="flow-layout"
                        id="runtime-panel"
                        role="tabpanel"
                        aria-labelledby=move || match active_flow.get() {
                            FlowKind::Command => "runtime-tab-command",
                            FlowKind::State => "runtime-tab-state",
                            FlowKind::Replication => "runtime-tab-replication",
                        }
                    >
                        <div class="flow-steps">
                            <For
                                each=move || flow_steps(active_flow.get()).iter().copied()
                                key=|step| step.title
                                children=move |step| view! {
                                    <article class="flow-step">
                                        <div class="flow-number">{step.number}</div>
                                        <div class="flow-body">
                                            <div class="flow-heading">
                                                <h3>{step.title}</h3>
                                                <span>{step.owner}</span>
                                            </div>
                                            <p>{step.detail}</p>
                                        </div>
                                    </article>
                                }
                            />
                        </div>

                        <div class="flow-aside" role="note">
                            <div class="flow-culture" aria-hidden="true">
                                <MyceliumField />
                                <Mark />
                            </div>
                            <div class="flow-note">
                                <span class="note-label">{move || flow_note(active_flow.get()).0}</span>
                                <p>{move || flow_note(active_flow.get()).1}</p>
                            </div>
                            <SourceBadge path="libs/myko/federation/src/lib.rs" />
                        </div>
                    </div>

                    <div class="lifecycle-strip">
                        <div>
                            <span class="lifecycle-dot lifecycle-dot-open"></span>
                            <small>"submitted"</small>
                        </div>
                        <i></i>
                        <div>
                            <span class="lifecycle-dot lifecycle-dot-active"></span>
                            <small>"executing"</small>
                        </div>
                        <i></i>
                        <div class="lifecycle-terminal-group">
                            <span class="lifecycle-dot lifecycle-dot-done"></span>
                            <small>"committed"</small>
                            <span class="lifecycle-alternates">"or rejected · retrying · cancelled"</span>
                        </div>
                        <div class="lifecycle-caption">"observable durable lifecycle"</div>
                    </div>
                </section>

                <section class="build section-pad" id="build">
                    <div class="section-intro">
                        <span class="section-number">"03"</span>
                        <div>
                            <p class="kicker">"Application shape"</p>
                            <h2>"Build vertical slices, not infrastructure wrappers."</h2>
                        </div>
                        <p class="section-summary">
                            "Forrest is the proof application: entities, commands, queries, and handlers live together; transport and supervision stay outside."
                        </p>
                    </div>

                    <div class="build-workbench">
                        <div class="build-tabs" role="tablist" aria-label="Application build steps">
                            <button
                                role="tab"
                                id="build-tab-model"
                                aria-controls="build-panel"
                                aria-selected=move || if active_build_tab.get() == BuildTab::Model { "true" } else { "false" }
                                class:active=move || active_build_tab.get() == BuildTab::Model
                                on:click=move |_| set_active_build_tab.set(BuildTab::Model)
                            ><span>"01"</span><b>"Model"</b><small>"service + item"</small></button>
                            <button
                                role="tab"
                                id="build-tab-command"
                                aria-controls="build-panel"
                                aria-selected=move || if active_build_tab.get() == BuildTab::Command { "true" } else { "false" }
                                class:active=move || active_build_tab.get() == BuildTab::Command
                                on:click=move |_| set_active_build_tab.set(BuildTab::Command)
                            ><span>"02"</span><b>"Intent"</b><small>"typed command"</small></button>
                            <button
                                role="tab"
                                id="build-tab-handler"
                                aria-controls="build-panel"
                                aria-selected=move || if active_build_tab.get() == BuildTab::Handler { "true" } else { "false" }
                                class:active=move || active_build_tab.get() == BuildTab::Handler
                                on:click=move |_| set_active_build_tab.set(BuildTab::Handler)
                            ><span>"03"</span><b>"Execute"</b><small>"bounded handler"</small></button>
                            <button
                                role="tab"
                                id="build-tab-compose"
                                aria-controls="build-panel"
                                aria-selected=move || if active_build_tab.get() == BuildTab::Compose { "true" } else { "false" }
                                class:active=move || active_build_tab.get() == BuildTab::Compose
                                on:click=move |_| set_active_build_tab.set(BuildTab::Compose)
                            ><span>"04"</span><b>"Compose"</b><small>"application node"</small></button>
                        </div>

                        <div
                            class="build-example"
                            id="build-panel"
                            role="tabpanel"
                            aria-labelledby=move || match active_build_tab.get() {
                                BuildTab::Model => "build-tab-model",
                                BuildTab::Command => "build-tab-command",
                                BuildTab::Handler => "build-tab-handler",
                                BuildTab::Compose => "build-tab-compose",
                            }
                        >
                            <div class="example-copy">
                                <span class="example-eyebrow">{move || build_example(active_build_tab.get()).eyebrow}</span>
                                <h3>{move || build_example(active_build_tab.get()).title}</h3>
                                <p>{move || build_example(active_build_tab.get()).detail}</p>
                                <div class="example-source">
                                    <span class="source-dot"></span>
                                    {move || build_example(active_build_tab.get()).source}
                                </div>
                            </div>
                            <div class="code-window">
                                <div class="code-toolbar">
                                    <span></span><span></span><span></span>
                                    <em>"forrest / rust"</em>
                                </div>
                                <pre><code>{move || build_example(active_build_tab.get()).code}</code></pre>
                            </div>
                        </div>
                    </div>

                    <div class="boundary-callout">
                        <div class="boundary-copy">
                            <span class="callout-tag">"THE APPLICATION BOUNDARY"</span>
                            <h3>"Domain intent in. Myko envelopes stay behind the adapter."</h3>
                            <p>"Types like `DeclaredCommand<C>`, transport `CommandRequest`, replication cursors, and event envelopes are execution machinery. A feature-facing API should expose `messenger.send(message)` and construct those details at its Myko edge."</p>
                        </div>
                        <div class="boundary-diagram">
                            <div class="boundary-public">
                                <small>"APP API"</small>
                                <code>"messenger.send(message)"</code>
                            </div>
                            <div class="boundary-gate">
                                <span></span><b>"adapter"</b><span></span>
                            </div>
                            <div class="boundary-internal">
                                <small>"MYKO INTERNAL"</small>
                                <code>"DeclaredCommand<SendAgentMessage>"</code>
                                <code>"CommandRequest + cursor"</code>
                            </div>
                        </div>
                    </div>
                </section>

                <section class="federation section-pad section-cream" id="federation">
                    <div class="section-intro">
                        <span class="section-number">"04"</span>
                        <div>
                            <p class="kicker">"Distributed environment"</p>
                            <h2>"One semantic core. Several ways to reach it."</h2>
                        </div>
                        <p class="section-summary">
                            "The node does not depend on a socket protocol. Sessions present one authorized request model across native, owner-local, and optional compatibility edges."
                        </p>
                    </div>

                    <div class="mesh-stage" role="group" aria-label="Distributed Myko node topology">
                        <MyceliumField />
                        <div class="mesh-node mesh-node-authority">
                            <div class="mesh-node-head">
                                <span class="mesh-status"></span>
                                <b>"Node A"</b>
                                <em>"authoritative"</em>
                            </div>
                            <div class="mesh-node-body">
                                <span>"ApplicationNode"</span>
                                <span>"Redb journal"</span>
                                <span>"Iroh endpoint"</span>
                            </div>
                            <div class="mesh-scope">"scope / agent:iris"</div>
                        </div>

                        <div class="mesh-node mesh-node-replica">
                            <div class="mesh-node-head">
                                <span class="mesh-status mesh-status-cyan"></span>
                                <b>"Node B"</b>
                                <em>"follower"</em>
                            </div>
                            <div class="mesh-node-body">
                                <span>"Remote projection"</span>
                                <span>"Durable cursor"</span>
                                <span>"Peer supervisor"</span>
                            </div>
                            <div class="mesh-scope">"source / node:a"</div>
                        </div>

                        <div class="mesh-client mesh-client-native">
                            <span>"native client"</span>
                            <small>"Iroh typed streams"</small>
                        </div>
                        <div class="mesh-client mesh-client-local">
                            <span>"local app"</span>
                            <small>"protected Unix socket"</small>
                        </div>
                        <div class="mesh-client mesh-client-web">
                            <span>"short-lived edge"</span>
                            <small>"optional WebSocket"</small>
                        </div>

                        <div class="mesh-link mesh-link-replication">
                            <i></i><span>"immutable batches + checkpoint"</span><i></i>
                        </div>
                        <div class="mesh-link mesh-link-native"><i></i></div>
                        <div class="mesh-link mesh-link-local"><i></i></div>
                        <div class="mesh-link mesh-link-web"><i></i></div>

                        <div class="mesh-legend">
                            <span><i class="legend-authority"></i>"executes local work"</span>
                            <span><i class="legend-replica"></i>"materializes remote facts"</span>
                            <span><i class="legend-edge"></i>"authorized request edge"</span>
                        </div>
                    </div>

                    <div class="federation-grid">
                        <article>
                            <span class="card-icon">"ID"</span>
                            <h3>"Identity before topology"</h3>
                            <p>"A native descriptor binds cryptographic transport identity to the stable source log. Pinned followers reject a different source before ingesting history."</p>
                            <SourceBadge path="libs/myko/iroh/src/lib.rs" />
                        </article>
                        <article>
                            <span class="card-icon">"AU"</span>
                            <h3>"Authorization stays live"</h3>
                            <p>"The principal comes from the authenticated session. Reads, pages, and open streams are authorized independently; policy revision can close a stream immediately."</p>
                            <SourceBadge path="libs/myko/session/src/lib.rs" />
                        </article>
                        <article>
                            <span class="card-icon">"RX"</span>
                            <h3>"Reconnect is a state"</h3>
                            <p>"Reactive clients keep their last coherent value, publish resynchronizing liveness, then rebuild from a fresh gap-free snapshot/follow boundary."</p>
                            <SourceBadge path="libs/myko/federation/src/reactive.rs" />
                        </article>
                    </div>

                    <div class="durable-node-band">
                        <div>
                            <p class="kicker">"The production composition"</p>
                            <h3>"Node"</h3>
                        </div>
                        <p>"Restores a stable Myko node ID, Iroh secret, immutable Redb journal, configured peers, and source-aware follower cursors from one data directory."</p>
                        <SourceBadge path="libs/myko/node/src/lib.rs" />
                    </div>
                </section>

                <section class="crate-atlas section-pad" id="crates">
                    <div class="section-intro section-intro-light">
                        <span class="section-number">"05"</span>
                        <div>
                            <p class="kicker">"Crate atlas"</p>
                            <h2>"Implementation boundaries, not application chores."</h2>
                        </div>
                        <p class="section-summary">
                            "The split keeps the semantic core transport-neutral while allowing storage, network, local, and UI mechanisms to evolve independently."
                        </p>
                    </div>

                    <div class="crate-list">
                        <For
                            each=move || CRATES.iter().copied()
                            key=|crate_info| crate_info.name
                            children=move |crate_info| {
                                let href = source_url(crate_info.path);
                                view! {
                                    <a class="crate-row" href=href target="_blank" rel="noreferrer">
                                        <span class="crate-layer">{crate_info.layer}</span>
                                        <h3>{crate_info.name}</h3>
                                        <p>{crate_info.summary}</p>
                                        <span class="crate-arrow"><ArrowIcon /></span>
                                    </a>
                                }
                            }
                        />
                    </div>

                    <div class="status-grid">
                        <article class="status-card status-shipped">
                            <div class="status-card-head">
                                <span></span>
                                <h3>"Proven in the alpha"</h3>
                            </div>
                            <ul>
                                <li>"Typed services, items, commands, queries, reports, and views"</li>
                                <li>"Durable command control and atomic item batches"</li>
                                <li>"Redb restart recovery and source-aware Iroh followers"</li>
                                <li>"Paginated current state and snapshot-then-live subscriptions"</li>
                                <li>"Scoped access control, revocation, peer supervision, and pairing"</li>
                            </ul>
                        </article>
                        <article class="status-card status-active">
                            <div class="status-card-head">
                                <span></span>
                                <h3>"Active design surface"</h3>
                            </div>
                            <ul>
                                <li>"Multi-writer reconciliation and selective coordination"</li>
                                <li>"Scoped readiness and coordinated invariants"</li>
                                <li>"Snapshots, retention, and richer cross-source derived views"</li>
                                <li>"Discovery, pairing UX, and production mobile/TUI clients"</li>
                                <li>"A cleaner application facade over internal command mechanics"</li>
                            </ul>
                        </article>
                    </div>
                </section>

                <section class="closing section-pad">
                    <MyceliumField />
                    <div class="closing-copy">
                        <p class="kicker">"The shortest useful definition"</p>
                        <h2>"Myko lets an application define what is true, who may change it, and how it should react—then carries those truths across restarts and nodes."</h2>
                        <div class="closing-actions">
                            <a class="button button-primary" href=REPOSITORY target="_blank" rel="noreferrer">
                                "Read the source" <ArrowIcon />
                            </a>
                            <a class="button button-ghost-light" href="#top">"Back to top"</a>
                        </div>
                    </div>
                </section>
            </main>

            <footer>
                <a class="brand brand-footer" href="#top"><Mark /><span class="brand-word">"myko"</span></a>
                <p>"Typed, event-sourced, federated applications."</p>
                <div class="footer-revision">
                    <span class="source-dot"></span>
                    {format!("source lens {SOURCE_BRANCH}@{SOURCE_REVISION} · {SOURCE_STATE}")}
                </div>
            </footer>
        </div>
    }
}

fn main() {
    mount_to_body(App);
}
