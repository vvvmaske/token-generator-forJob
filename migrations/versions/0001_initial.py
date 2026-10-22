from alembic import op
revision='0001_initial'; down_revision=None; branch_labels=None; depends_on=None
def upgrade():
 op.execute("""
 CREATE TABLE users(id text PRIMARY KEY, balance bigint NOT NULL DEFAULT 0 CHECK(balance>=0));
 CREATE TABLE ledger(id bigserial PRIMARY KEY, user_id text NOT NULL REFERENCES users(id), amount bigint NOT NULL, reason text NOT NULL, reference text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(reason,reference));
 CREATE TABLE models(slug text PRIMARY KEY, price bigint NOT NULL);
 CREATE TABLE legs(id bigserial PRIMARY KEY, model_slug text NOT NULL REFERENCES models(slug), provider text NOT NULL, priority int NOT NULL, max_ref_images int NOT NULL, skip_on_moderation boolean NOT NULL DEFAULT false, UNIQUE(model_slug,priority));
 CREATE TABLE tasks(id uuid PRIMARY KEY, user_id text NOT NULL REFERENCES users(id), model_slug text NOT NULL REFERENCES models(slug), prompt text NOT NULL, ref_images jsonb NOT NULL, status text NOT NULL, current_leg int NOT NULL DEFAULT 0, last_error text, result_url text, created_at timestamptz NOT NULL DEFAULT now());
 CREATE TABLE attempts(id bigserial PRIMARY KEY, task_id uuid NOT NULL REFERENCES tasks(id), leg_id bigint NOT NULL REFERENCES legs(id), provider text NOT NULL, external_id text, status text NOT NULL, error_type text, started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz, UNIQUE(provider,external_id));
 CREATE TABLE payments(payment_id text PRIMARY KEY, user_id text NOT NULL REFERENCES users(id), tokens bigint NOT NULL, status text NOT NULL);
 INSERT INTO models VALUES ('image-fast',2),('video-long',25);
 INSERT INTO legs(model_slug,provider,priority,max_ref_images) VALUES ('image-fast','alpha',0,4),('image-fast','beta',1,10),('video-long','beta',0,2),('video-long','alpha',1,1);
 UPDATE legs SET skip_on_moderation=true WHERE model_slug='video-long' AND priority=1;
 """)
def downgrade(): op.execute("DROP TABLE attempts,payments,tasks,legs,models,ledger,users CASCADE")
