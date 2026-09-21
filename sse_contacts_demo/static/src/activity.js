import { Component, signal, useEffect, useProps, xml } from "@odoo/owl";
import { registry } from "@web/core/registry";
import { standardActionServiceProps } from "@web/webclient/actions/action_plugin";
import { subscribe } from "@sse_server/client";

class LiveContactsActivity extends Component {
    props = useProps({ ...standardActionServiceProps });
    count = signal(null);
    events = signal([]);
    connection = signal("connecting");
    database = signal("");

    static template = xml`
        <div class="h-100 overflow-auto bg-light p-4">
            <div class="d-flex justify-content-between align-items-center mb-4">
                <div>
                    <h1 class="h3 mb-1">Live Contacts Activity</h1>
                    <span class="text-muted">Create, update and delete events · <t t-esc="this.database()"/></span>
                </div>
                <a class="btn btn-primary" href="/odoo/contacts" target="_blank" rel="noopener">Open Contacts ↗</a>
            </div>
            <div class="row g-3 mb-4">
                <div class="col-md-6"><div class="card p-4 h-100">
                    <span class="text-muted">Total contacts</span>
                    <strong class="display-5" data-contact-count=""><t t-esc="this.count() ?? '…'"/></strong>
                    <small class="text-muted">Includes archived contacts, companies and addresses</small>
                </div></div>
                <div class="col-md-6"><div class="card p-4 h-100">
                    <span class="text-muted">Connection</span>
                    <strong class="h4 my-2" t-att-class="this.connection() === 'live' ? 'text-success' : 'text-warning'"
                            data-connection=""><t t-esc="this.connection()"/></strong>
                    <small class="text-muted">Live events appear here as changes are saved.</small>
                </div></div>
            </div>
            <div class="card">
                <div class="card-header bg-white py-3 d-flex justify-content-between">
                    <strong>Recent activity</strong><span class="text-muted">Latest 100 received events</span>
                </div>
                <div t-if="!this.events().length" class="p-5 text-center text-muted">
                    Waiting for changes. Create, edit or delete a contact in another tab.
                </div>
                <div t-else="" class="table-responsive">
                    <table class="table mb-0 align-middle"><thead><tr>
                        <th class="ps-4">Time</th><th>Operation</th><th>Contact IDs</th><th>User ID</th><th>Fields</th>
                    </tr></thead><tbody>
                        <tr t-foreach="this.events()" t-as="event" t-key="event.id" data-activity-row="">
                            <td class="ps-4 text-nowrap"><t t-esc="event.time.slice(11, 19)"/> UTC</td>
                            <td><span class="badge text-bg-light"><t t-esc="event.operation"/></span></td>
                            <td><t t-esc="event.record_ids.join(', ')"/></td>
                            <td><t t-esc="event.actor_user_id"/></td>
                            <td class="text-muted"><t t-esc="event.fields.join(', ') || '—'"/></td>
                        </tr>
                    </tbody></table>
                </div>
            </div>
            <p class="small text-muted mt-3">Activity is live and temporary. Reconnecting refreshes the total; missed events are not replayed.</p>
        </div>`;

    setup() {
        useEffect(() => subscribe({
            stream: "contacts.activity",
            snapshotRoute: "/sse_contacts_demo/snapshot",
            onCount: (value) => this.count.set(value),
            onEvent: (event) => this.events.set([event, ...this.events()].slice(0, 100)),
            onState: (state, database) => {
                this.connection.set(state);
                if (database) this.database.set(database);
            },
        }));
    }
}

registry.category("actions").add("sse_contacts_demo.activity", LiveContactsActivity);
