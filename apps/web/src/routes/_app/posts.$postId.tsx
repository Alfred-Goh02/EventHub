import { createFileRoute, Link } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState, type FormEvent } from "react";
import { Loader2, MessageSquare } from "lucide-react";
import { api, ApiError, getSession, type Comment } from "@/api";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Textarea } from "@/components/ui/textarea";
import { ErrorNotice, retryText } from "@/components/app/ErrorNotice";
import { formatDateTime } from "@/lib/format";
import { authorLabel, commentError } from "@/lib/thread";

export const Route = createFileRoute("/_app/posts/$postId")({
  head: () => ({ meta: [{ title: "Conversation — EventHub" }] }),
  component: PostPage,
});

function PostPage() {
  const { postId } = Route.useParams();
  const me = getSession()?.claims.sub ?? null;
  const post = useQuery({
    queryKey: ["post", postId],
    queryFn: () => api().getPost(postId),
    retry: (n, err) => !(err instanceof ApiError && [403, 404].includes(err.status)) && n < 3,
  });
  const thread = useQuery({
    queryKey: ["thread", postId],
    queryFn: () => api().getThread(postId),
    enabled: post.isSuccess,
  });

  if (post.isPending) {
    return (
      <div className="space-y-3">
        <Skeleton className="h-8 w-1/2" />
        <Skeleton className="h-32 w-full rounded-xl" />
      </div>
    );
  }
  if (post.isError) {
    const status = post.error instanceof ApiError ? post.error.status : 0;
    if (status === 404 || status === 403) {
      return (
        <div className="rounded-xl border border-dashed p-10 text-center">
          <MessageSquare className="mx-auto mb-2 h-6 w-6 text-muted-foreground" />
          <p className="font-medium">This conversation isn't available.</p>
          <p className="mt-1 text-sm text-muted-foreground">
            {status === 403
              ? "You can no longer take part in this space."
              : "It may have been removed, or it's in a space you're not part of."}
          </p>
          <Link
            to="/"
            className="mt-4 inline-block text-sm text-primary underline-offset-4 hover:underline"
          >
            Back to events
          </Link>
        </div>
      );
    }
    return <ErrorNotice error={post.error} />;
  }

  const p = post.data;
  return (
    <article className="mx-auto max-w-2xl">
      <p className="text-sm text-muted-foreground">{p.space.name}</p>
      <div className="mt-2 rounded-xl border bg-card p-5">
        <p className="text-sm text-muted-foreground">
          {authorLabel(p.authorId, me)} · {formatDateTime(p.createdAt)}
        </p>
        <p className="mt-2 whitespace-pre-wrap break-words">{p.body}</p>
      </div>

      <h2 className="mt-8 text-lg font-semibold">
        {p.commentCount === 1 ? "1 reply" : `${p.commentCount} replies`}
      </h2>

      {p.canReply ? (
        <ReplyForm postId={postId} className="mt-3" />
      ) : (
        <p className="mt-3 rounded-lg bg-secondary px-4 py-3 text-sm text-muted-foreground">
          Only members of {p.space.name} can reply.
        </p>
      )}

      <div className="mt-6">
        {thread.isPending && <Skeleton className="h-20 w-full rounded-xl" />}
        {thread.isError && <ErrorNotice error={thread.error} />}
        {thread.isSuccess && thread.data.comments.length === 0 && (
          <p className="text-sm text-muted-foreground">No replies yet.</p>
        )}
        {thread.isSuccess && (
          <ul className="space-y-3">
            {thread.data.comments.map((c) => (
              <CommentItem key={c.id} comment={c} postId={postId} me={me} canReply={p.canReply} />
            ))}
          </ul>
        )}
        {thread.isSuccess && thread.data.truncated && (
          <p className="mt-4 text-sm text-muted-foreground">
            Showing the first {thread.data.count} replies.
          </p>
        )}
      </div>
    </article>
  );
}

function CommentItem({
  comment,
  postId,
  me,
  canReply,
}: {
  comment: Comment;
  postId: string;
  me: string | null;
  canReply: boolean;
}) {
  const [replying, setReplying] = useState(false);
  return (
    <li>
      <div className="rounded-lg border bg-card px-4 py-3">
        {comment.deleted ? (
          <p className="text-sm italic text-muted-foreground">This reply was deleted.</p>
        ) : (
          <>
            <p className="text-xs text-muted-foreground">
              {authorLabel(comment.authorId, me)} · {formatDateTime(comment.createdAt)}
            </p>
            <p className="mt-1 whitespace-pre-wrap break-words text-sm">{comment.body}</p>
            {canReply && !replying && (
              <button
                type="button"
                className="mt-1 text-xs text-muted-foreground underline-offset-4 hover:underline"
                onClick={() => setReplying(true)}
              >
                Reply
              </button>
            )}
          </>
        )}
      </div>
      {replying && (
        <ReplyForm
          postId={postId}
          parentId={comment.id}
          className="ml-4 mt-2"
          onDone={() => setReplying(false)}
        />
      )}
      {comment.replies.length > 0 && (
        <ul className="ml-4 mt-2 space-y-2 border-l pl-3">
          {comment.replies.map((r) => (
            <CommentItem key={r.id} comment={r} postId={postId} me={me} canReply={canReply} />
          ))}
        </ul>
      )}
    </li>
  );
}

function ReplyForm({
  postId,
  parentId,
  className,
  onDone,
}: {
  postId: string;
  parentId?: string;
  className?: string;
  onDone?: () => void;
}) {
  const qc = useQueryClient();
  const [body, setBody] = useState("");
  const send = useMutation({
    mutationFn: () => api().addComment(postId, body.trim(), parentId),
    onSuccess: async () => {
      setBody("");
      onDone?.();
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["thread", postId] }),
        qc.invalidateQueries({ queryKey: ["post", postId] }),
      ]);
    },
  });
  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (body.trim()) send.mutate();
  };
  const err = send.error instanceof ApiError ? send.error : null;
  const message = err ? commentError(err.status, retryText(err)) : undefined;
  return (
    <form onSubmit={submit} className={className}>
      <Textarea
        value={body}
        onChange={(e) => setBody(e.target.value)}
        placeholder={parentId ? "Write a reply" : "Add to the conversation"}
        maxLength={10_000}
        rows={parentId ? 2 : 3}
        aria-label={parentId ? "Your reply" : "Your reply to the post"}
      />
      {send.isError && <ErrorNotice error={send.error} message={message} className="mt-2" />}
      <div className="mt-2 flex gap-2">
        <Button type="submit" size="sm" disabled={send.isPending || !body.trim()}>
          {send.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
          Reply
        </Button>
        {onDone && (
          <Button type="button" size="sm" variant="ghost" onClick={onDone}>
            Cancel
          </Button>
        )}
      </div>
    </form>
  );
}
