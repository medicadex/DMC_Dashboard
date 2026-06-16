import os
from PIL import Image, ImageTk, ImageEnhance
import tkinter as tk
from tkinter import ttk

class SearchableMultiSelect(tk.Frame):
    """
    A custom Tkinter widget that provides a searchable multi-select listbox.
    Features:
    - Live search at the top.
    - Ctrl+Up/Down keyboard navigation.
    - Space/Enter to toggle selection.
    - Select All / Deselect All buttons.
    """
    def __init__(self, parent, label_text, items=[], height=5, width=35, on_change=None, **kwargs):
        super().__init__(parent, bg="white", **kwargs)
        self.all_items = items
        self.filtered_items = items
        self.selected_items = set()
        self.on_change = on_change
        
        # 1. Label and Select All/None controls
        header_f = tk.Frame(self, bg="white")
        header_f.pack(fill="x")
        
        tk.Label(header_f, text=label_text, bg="white", font=("Segoe UI", 9, "bold")).pack(side="left")
        
        btn_f = tk.Frame(header_f, bg="white")
        btn_f.pack(side="right")
        
        tk.Button(btn_f, text="☑", command=self.select_all, font=("Segoe UI", 8), bg="#f0f0f0", padx=2, pady=0).pack(side="left", padx=1)
        tk.Button(btn_f, text="☐", command=self.deselect_all, font=("Segoe UI", 8), bg="#f0f0f0", padx=2, pady=0).pack(side="left", padx=1)

        # 2. Search Entry
        self.search_var = tk.StringVar()
        self.search_ent = ttk.Entry(self, textvariable=self.search_var, font=("Segoe UI", 9))
        self.search_ent.pack(fill="x", pady=(2, 5))
        self.search_ent.insert(0, "🔍 Search...")
        self.search_ent.bind("<FocusIn>", self._on_focus_in)
        self.search_ent.bind("<FocusOut>", self._on_focus_out)

        # 3. Listbox with Scrollbar
        lb_frame = tk.Frame(self, bg="white")
        lb_frame.pack(fill="both", expand=True)
        
        self.lb = tk.Listbox(lb_frame, selectmode="multiple", height=height, width=width, 
                             font=("Segoe UI", 9), relief="flat", highlightthickness=1,
                             exportselection=0) # Critical: don't lose selection on focus loss
        self.lb.pack(side="left", fill="both", expand=True)
        
        sb = ttk.Scrollbar(lb_frame, orient="vertical", command=self.lb.yview)
        sb.pack(side="right", fill="y")
        self.lb.config(yscrollcommand=sb.set)
        
        # 4. Bindings
        self.lb.bind("<<ListboxSelect>>", self._on_lb_select)
        self.lb.bind("<Control-Up>", lambda e: self._move_focus(-1))
        self.lb.bind("<Control-Down>", lambda e: self._move_focus(1))
        self.lb.bind("<space>", lambda e: self._toggle_selected())
        self.lb.bind("<Return>", lambda e: self._toggle_selected())
        
        # 5. Initialize Search Trace (Do this after self.lb is ready)
        self.search_var.trace_add("write", lambda *args: self.filter_items())
        
        self._refresh_lb()

    def _on_focus_in(self, event):
        if self.search_var.get() == "🔍 Search...":
            self.search_var.set("")

    def _on_focus_out(self, event):
        if not self.search_var.get():
            self.search_var.set("🔍 Search...")

    def set_items(self, items):
        self.all_items = items
        self.filter_items()

    def filter_items(self):
        query = self.search_var.get().lower()
        if query == "🔍 search...": query = ""
        
        self.filtered_items = [i for i in self.all_items if query in str(i).lower()]
        self._refresh_lb()

    def _refresh_lb(self):
        if not hasattr(self, 'lb'): return # Defensive check
        self.lb.delete(0, tk.END)
        for i, item in enumerate(self.filtered_items):
            self.lb.insert(tk.END, item)
            if item in self.selected_items:
                self.lb.select_set(i)
        
        # Maintain focus if items exist
        if self.lb.size() > 0:
            self.lb.activate(0)

    def _on_lb_select(self, event):
        # This only handles mouse clicks
        current_indices = self.lb.curselection()
        current_filtered = set([self.filtered_items[i] for i in current_indices])
        
        # Logic: Update global selected_items based on what's visible in filtered list
        # 1. Remove filtered items that are NOT selected in listbox
        for item in self.filtered_items:
            if item in self.selected_items and item not in current_filtered:
                self.selected_items.remove(item)
        
        # 2. Add filtered items that ARE selected in listbox
        for item in current_filtered:
            self.selected_items.add(item)
            
        if self.on_change:
            self.on_change()

    def _move_focus(self, delta):
        idx = self.lb.index(tk.ACTIVE)
        new_idx = max(0, min(self.lb.size() - 1, idx + delta))
        self.lb.activate(new_idx)
        self.lb.see(new_idx)
        return "break"

    def _toggle_selected(self):
        idx = self.lb.index(tk.ACTIVE)
        if idx < 0: return
        
        item = self.filtered_items[idx]
        if item in self.selected_items:
            self.selected_items.remove(item)
            self.lb.select_clear(idx)
        else:
            self.selected_items.add(item)
            self.lb.select_set(idx)
            
        if self.on_change:
            self.on_change()
        return "break"

    def select_all(self):
        for item in self.filtered_items:
            self.selected_items.add(item)
        self._refresh_lb()
        if self.on_change: self.on_change()

    def deselect_all(self):
        for item in self.filtered_items:
            if item in self.selected_items:
                self.selected_items.remove(item)
        self._refresh_lb()
        if self.on_change: self.on_change()

    def get_selected(self):
        return list(self.selected_items)

    def clear(self):
        self.selected_items.clear()
        self.search_var.set("")
        self._refresh_lb()

class PaginatedTreeview(tk.Frame):
    """
    A reusable Tkinter component that wraps a Treeview with pagination controls.
    Enforces a strict row limit per page and provides navigation.
    """
    def __init__(self, parent, page_size=1000, on_page_change=None, **kwargs):
        super().__init__(parent, bg="white")
        self.page_size = page_size
        self.on_page_change = on_page_change # Callback(page_index)
        self.current_page = 0
        self.total_records = 0
        self.total_pages = 0
        
        self.setup_ui()
        self._bind_keys()

    def setup_ui(self):
        # 1. Main Container
        self.container = tk.Frame(self, bg="white")
        self.container.pack(fill="both", expand=True)
        
        # 2. Treeview + Scrollbars
        self.tree = ttk.Treeview(self.container, show="headings", selectmode="extended")
        self.vsb = ttk.Scrollbar(self.container, orient="vertical", command=self.tree.yview)
        self.hsb = ttk.Scrollbar(self.container, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=self.vsb.set, xscrollcommand=self.hsb.set)
        
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.vsb.grid(row=0, column=1, sticky="ns")
        self.hsb.grid(row=1, column=0, sticky="ew")
        
        self.container.grid_columnconfigure(0, weight=1)
        self.container.grid_rowconfigure(0, weight=1)
        
        # 3. Pagination Control Bar (Desktop/Default)
        self.pager_frame = tk.Frame(self, bg="#f8f9fa", pady=5)
        self.pager_frame.pack(fill="x", side="bottom")
        
        # Left side: Page info
        self.page_info_lbl = tk.Label(self.pager_frame, text="Page 0 of 0 (Total: 0)", 
                                     font=("Segoe UI", 9), bg="#f8f9fa", fg="#666")
        self.page_info_lbl.pack(side="left", padx=20)
        
        # Right side: Navigation buttons
        self.nav_frame = tk.Frame(self.pager_frame, bg="#f8f9fa")
        self.nav_frame.pack(side="right", padx=20)
        
        self.first_btn = tk.Button(self.nav_frame, text="«", command=lambda: self.go_to_page(0), 
                                  bg="#f8f9fa", relief="flat", width=3)
        self.first_btn.pack(side="left", padx=2)
        
        self.prev_btn = tk.Button(self.nav_frame, text="‹", command=lambda: self.go_to_page(self.current_page - 1), 
                                 bg="#f8f9fa", relief="flat", width=3)
        self.prev_btn.pack(side="left", padx=2)
        
        # Page Number Entry for direct jump
        tk.Label(self.nav_frame, text="Go to:", bg="#f8f9fa", font=("Segoe UI", 8)).pack(side="left", padx=(10, 2))
        self.page_entry_var = tk.StringVar()
        self.page_entry = ttk.Entry(self.nav_frame, textvariable=self.page_entry_var, width=5)
        self.page_entry.pack(side="left", padx=2)
        self.page_entry.bind("<Return>", lambda e: self._jump_to_page())
        
        self.next_btn = tk.Button(self.nav_frame, text="›", command=lambda: self.go_to_page(self.current_page + 1), 
                                 bg="#f8f9fa", relief="flat", width=3)
        self.next_btn.pack(side="left", padx=2)
        
        self.last_btn = tk.Button(self.nav_frame, text="»", command=lambda: self.go_to_page(self.total_pages - 1), 
                                 bg="#f8f9fa", relief="flat", width=3)
        self.last_btn.pack(side="left", padx=2)

        # Loading Spinner (Animated Label)
        self.spinner_lbl = tk.Label(self.pager_frame, text="", bg="#f8f9fa", fg="#2B5797", font=("Segoe UI", 10, "bold"))
        self.spinner_lbl.pack(side="right", padx=10)
        self._spinner_chars = ["|", "/", "-", "\\"]
        self._spinner_idx = 0
        self._spinning = False

    def _bind_keys(self):
        # Keyboard Accessibility - Bind to tree and pager container
        self.tree.bind("<Left>", lambda e: self._handle_arrow_nav("left"))
        self.tree.bind("<Right>", lambda e: self._handle_arrow_nav("right"))
        # Also bind to the frame itself if focused
        self.bind("<Left>", lambda e: self._handle_arrow_nav("left"))
        self.bind("<Right>", lambda e: self._handle_arrow_nav("right"))
        
    def _handle_arrow_nav(self, direction):
        # Only navigate if typing isn't happening in the page entry
        focused = self.focus_get()
        if focused == self.page_entry: return
        
        if direction == "left":
            self.go_to_page(self.current_page - 1)
        else:
            self.go_to_page(self.current_page + 1)

    def _jump_to_page(self):
        try:
            val = int(self.page_entry_var.get()) - 1
            self.go_to_page(val)
        except:
            self.page_entry_var.set(str(self.current_page + 1))

    def update_metadata(self, total_records, current_page=None):
        self.total_records = total_records
        self.total_pages = max(1, (total_records + self.page_size - 1) // self.page_size)
        if current_page is not None:
            self.current_page = current_page
        
        self.page_info_lbl.config(text=f"Page {self.current_page + 1} of {self.total_pages} (Total: {total_records:,})")
        self.page_entry_var.set(str(self.current_page + 1))
        
        # Update button states
        self.first_btn.config(state="normal" if self.current_page > 0 else "disabled")
        self.prev_btn.config(state="normal" if self.current_page > 0 else "disabled")
        self.next_btn.config(state="normal" if self.current_page < self.total_pages - 1 else "disabled")
        self.last_btn.config(state="normal" if self.current_page < self.total_pages - 1 else "disabled")

    def go_to_page(self, page_index):
        if 0 <= page_index < self.total_pages and page_index != self.current_page:
            self.current_page = page_index
            if self.on_page_change:
                self.start_loading()
                self.on_page_change(page_index)

    def start_loading(self):
        self._spinning = True
        self._animate_spinner()
        for btn in [self.first_btn, self.prev_btn, self.next_btn, self.last_btn]:
            btn.config(state="disabled")

    def stop_loading(self):
        self._spinning = False
        self.spinner_lbl.config(text="")
        self.update_metadata(self.total_records) # Refresh button states

    def _animate_spinner(self):
        if not self._spinning: return
        self.spinner_lbl.config(text=self._spinner_chars[self._spinner_idx])
        self._spinner_idx = (self._spinner_idx + 1) % len(self._spinner_chars)
        self.after(100, self._animate_spinner)

    def set_columns(self, cols, amount_cols=None):
        self.tree["columns"] = cols
        for col in cols:
            self.tree.heading(col, text=col)
            # Default logic for widths and alignment
            is_amt = amount_cols and col in amount_cols
            self.tree.column(col, width=150, anchor="e" if is_amt else "w", stretch=False)

    def clear(self):
        for i in self.tree.get_children():
            self.tree.delete(i)

    def insert_row(self, values, tags=None):
        self.tree.insert("", "end", values=values, tags=tags)

def get_logo_image(size=(400, 400), alpha=0.1):
    """
    Loads IE LOGO.jpg, resizes it, and applies transparency.
    Returns a PhotoImage compatible with Tkinter.
    """
    from db_utils import get_project_folder
    logo_path = os.path.join(get_project_folder(), "IE LOGO.jpg")
    
    if not os.path.exists(logo_path):
        return None
        
    try:
        img = Image.open(logo_path).convert("RGBA")
        img = img.resize(size, Image.Resampling.LANCZOS)
        
        # Apply transparency (watermark effect)
        alpha_channel = img.getchannel('A')
        alpha_channel = alpha_channel.point(lambda i: int(i * alpha))
        img.putalpha(alpha_channel)
        
        return ImageTk.PhotoImage(img)
    except Exception as e:
        print(f"Error processing logo: {e}")
        return None

def apply_watermark(parent, photo_image, alpha=0.1):
    """
    Creates a label for the watermark and places it in the center of the parent.
    Then lowers it so it's behind other widgets.
    """
    if not photo_image:
        return
        
    # Standard Tkinter widgets use 'bg' or 'background'
    # ttk widgets might not support cget("bg")
    try:
        bg_color = parent.cget("bg")
    except:
        bg_color = "white" # Fallback
        
    lbl = tk.Label(parent, image=photo_image, bg=bg_color)
    lbl.image = photo_image # keep a reference
    lbl.place(relx=0.5, rely=0.5, anchor="center")
    lbl.lower()
    return lbl
