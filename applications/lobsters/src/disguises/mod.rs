pub mod baseline;
/*  def delete!
    User.transaction do
      self.comments
        .where("score < 0")
        .find_each {|c| c.delete_for_user(self) }

      self.sent_messages.each do |m|
        m.deleted_by_author = true
        m.save
      end
      self.received_messages.each do |m|
        m.deleted_by_recipient = true
        m.save
      end

      self.invitations.destroy_all

      self.session_token = nil
      self.check_session_token

      self.deleted_at = Time.current
      self.good_riddance?
      self.save!
    end
  end

  def undelete!
    User.transaction do
      self.sent_messages.each do |m|
        m.deleted_by_author = false
        m.save
      end
      self.received_messages.each do |m|
        m.deleted_by_recipient = false
        m.save
      end

      self.deleted_at = nil
      self.save!
    end
  end

  # ensures some users talk to a mod before reactivating
  def good_riddance?
    return if self.is_banned? # https://www.youtube.com/watch?v=UcZzlPGnKdU
    self.email = "#{self.username}@lobsters.example" if \
      self.karma < 0 ||
      (self.comments.where('created_at >= now() - interval 30 day AND is_deleted').count +
       self.stories.where('created_at >= now() - interval 30 day AND is_expired AND is_moderated')
         .count >= 3) ||
      FlaggedCommenters.new('90d').check_list_for(self)
  end

  def self.disown_all_by_author! author
    author.stories.update_all(:user_id => inactive_user.id)
    author.comments.update_all(:user_id => inactive_user.id)
    refresh_counts! author
  end
*/
